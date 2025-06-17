#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Vérification de la conformité d'un serveur de fichier ARBOMUT"""

import argparse
import hashlib
import logging

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import writer
from datetime import datetime
from os import scandir, cpu_count
from os.path import getsize
from pathlib import Path
from re import compile
from sys import stdout
from unicodedata import normalize

LONGUEUR_MAXIMALE_FICHIER = 255
REGEX_DOSSIER_VIDE = compile(r"^[\w ]+-VIDE$")
REGEX_NIVEAU_1 = compile(r"^[0-9]{2}_[A-Z]{3}_[\w\s-]+$")
REGEX_NIVEAU_2 = compile(r"^(Z_)?[0-9]{6}_[A-Z]+_\d+_[\w\s-]+$")
# REGEX_NIVEAU_3 = compile(r"^([A-Z])\w+$")


class SentinelleErreur(Exception):
    pass


class Sentinelle:
    def __init__(self, chemin_in: Path, chemin_out: Path, profondeur_max: int) -> None:
        """Initialisation de l'objet"""
        self._chemin_in = chemin_in
        self._niveau_chemin_in = len(chemin_in.parents)
        self._chemin_out = chemin_out
        self._profondeur_max = profondeur_max
        # vérification des chemins
        self._verifie_chemins()
        # variables
        self._now = datetime.now()
        self._trop_long = []
        self._mauvais_nom = []
        self._vide = []
        self._non_vide = []
        self._hash_map = defaultdict(list)
        self._dupliques = {}

    def _verifie_chemins(self) -> None:
        """Vérifie que les chemins d'entrée et de sortie fonctionnent"""
        # le chemin d'entrée est un dossier
        if not self._chemin_in.is_dir():
            logging.critical("le chemin d'entrée n'existe pas ou n'est pas un dossier")
            raise SentinelleErreur()

        # le chemin de sortie est un dossier, on le crée s'il n'existe pas
        if not self._chemin_out.is_dir():
            try:
                logging.info("création du dossier de sortie")
                self._chemin_out.mkdir(parents=True)
            except PermissionError:
                logging.critical(
                    "le programme n'a pas les droits d'écriture sur le chemin de sortie"
                )
                raise SentinelleErreur()
        else:
            # test des droits d'écriture
            try:
                (self._chemin_out / "test").touch()
            except PermissionError:
                logging.critical(
                    "le programme n'a pas les droits d'écriture sur le chemin de sortie"
                )
                raise SentinelleErreur()
            else:
                (self._chemin_out / "test").unlink()

    @staticmethod
    def _hash_file(chemin: str):
        """Retourne (hash, path) ou (None, path) si erreur."""
        try:
            hasher = hashlib.blake2b(digest_size=32)
            with open(chemin, "rb") as f:
                chunk = f.read(8192)
                while chunk:
                    hasher.update(chunk)
                    chunk = f.read(8192)
            return hasher.hexdigest(), chemin
        except Exception as e:
            logging.debug(e)
            return None, chemin

    def _verif_fichier_longueur(self, fichier: str):
        """vérifie que le fichier n'a pas un chemin trop long"""
        if len(fichier) > LONGUEUR_MAXIMALE_FICHIER:
            self._trop_long.append(fichier)

    def _verif_dossier_nom(self, dossier: Path):
        """vérifie que le nom d'un dossier est cohérent"""
        niveau = len(dossier.parents) - self._niveau_chemin_in
        if niveau == 1:  # cf note ARBOMUT chap. 4.2
            if not REGEX_NIVEAU_1.match(dossier.name):
                self._mauvais_nom.append((niveau, str(dossier)))
        elif niveau == 2:  # cf note ARBOMUT chap. 4.3
            if not REGEX_NIVEAU_2.match(dossier.name):
                self._mauvais_nom.append((niveau, str(dossier)))
            if len(dossier.name) > 50:
                self._trop_long.append(str(dossier))
        # elif niveau == 3:
        #    if not REGEX_NIVEAU_3.match(dossier.name):
        #        self._mauvais_nom.append((niveau, str(dossier)))

    @staticmethod
    def _is_dossier_non_vide(dossier: Path):
        """vérifie qu'un dossier est vide tout élément"""
        try:
            for _ in scandir(dossier):
                return True
        except PermissionError:
            pass
        except FileNotFoundError:
            pass  # bug: si le chemin est trop long, le fichier n'est pas lisible
        return False  # Aucun fichier ou dossier trouvé dans ce dossier ni ses sous-dossiers

    def _verif_dossier_vide(self, dossier: Path):
        """vérifie qu'un dossier se prétendant vide l'est bien (et inversement)"""
        if REGEX_DOSSIER_VIDE.match(str(dossier.name)):
            if self._is_dossier_non_vide(dossier):
                self._vide.append(str(dossier))
        else:
            if not self._is_dossier_non_vide(dossier):
                self._non_vide.append(str(dossier))

    def _scanne(self, chemin_racine: Path):
        stack = [chemin_racine]
        # plusieurs threads pour le calculs des hash
        with ThreadPoolExecutor(max_workers=cpu_count() or 4) as executor:
            futures = []
            # tant qu'il reste des dossiers à scanner
            while stack:
                current = stack.pop()
                try:
                    # scanne le dossier
                    for entry in scandir(current):
                        entry_path = Path(entry.path)

                        # si le scanné est un dossier, on l'ajoute à la pile des dossiers à scanner
                        if entry.is_dir(follow_symlinks=False):
                            # vérifie que le dossier n'est pas trop profond
                            if (
                                len(entry_path.parents) - self._niveau_chemin_in
                                <= self._profondeur_max
                            ):
                                stack.append(entry.path)
                            # vérification du regex
                            self._verif_dossier_nom(entry_path)
                            # vérifie s'il est vide
                            self._verif_dossier_vide(entry_path)

                        # si le scanné est un fichier, on procède aux vérifications
                        elif entry.is_file(follow_symlinks=False):
                            # vérification du chemin UNC
                            self._verif_fichier_longueur(
                                entry.path
                            )  # besoin d'un 'str'
                            # lance le calcul du hash
                            futures.append(executor.submit(self._hash_file, entry.path))

                except PermissionError:
                    continue
                except FileNotFoundError:
                    pass  # bug: si le chemin est trop long, le fichier n'est pas lisible

        # Détermination des doublons (attente de tous les hachages si besoin)
        for future in as_completed(futures):
            hash_result, path = future.result()
            if hash_result:
                self._hash_map[hash_result].append(path)
        self._dupliques.update(
            {h: paths for h, paths in self._hash_map.items() if len(paths) > 1}
        )

    def _exporte_csv(self):
        """exporte les résultats dans des fichiers CSV"""
        # Export dossiers avec noms invalides
        str_date = self._now.strftime("%Y-%m-%d %H%M%S")
        str_nom = self._chemin_in.stem
        if self._mauvais_nom:
            with open(
                self._chemin_out
                / f"{str_nom} {str_date} {str_nom} dossiers mal nommés.csv",
                "w",
                newline="",
                encoding="cp1252",
            ) as f:
                w = writer(f, delimiter=";")
                w.writerow(["niveau", "chemin"])
                for niv, path in self._mauvais_nom:
                    path_norm = normalize("NFC", str(path))
                    w.writerow([niv, path_norm])

        # Export dossiers non vides (qui se font passer pour vide)
        if self._vide:
            with open(
                self._chemin_out
                / f"{str_nom} {str_date} {str_nom} dossiers -VIDE qui ne le sont pas.csv",
                "w",
                newline="",
                encoding="cp1252",
            ) as f:
                w = writer(f, delimiter=";")
                w.writerow(["chemin"])
                for path in self._vide:
                    path_norm = normalize("NFC", str(path))
                    w.writerow([path_norm])

        # Export dossiers vides (qui se font passer pour non vide)
        if self._non_vide:
            with open(
                self._chemin_out
                / f"{str_nom} {str_date} {str_nom} dossiers sans -VIDE qui sont vides.csv",
                "w",
                newline="",
                encoding="cp1252",
            ) as f:
                w = writer(f, delimiter=";")
                w.writerow(["chemin"])
                for path in self._non_vide:
                    path_norm = normalize("NFC", str(path))
                    w.writerow([path_norm])

        # Export fichiers trop longs
        if self._trop_long:
            with open(
                self._chemin_out
                / f"{str_nom} {str_date} {str_nom} fichiers trop longs.csv",
                "w",
                newline="",
                encoding="cp1252",
            ) as f:
                w = writer(f, delimiter=";")
                w.writerow(["chemin"])
                for path in self._trop_long:
                    path_norm = normalize("NFC", str(path))
                    w.writerow([path_norm])

        # Export doublons (hash -> liste des fichiers)
        if any(len(paths) > 1 for paths in self._hash_map.values()):
            with open(
                self._chemin_out
                / f"{str_nom} {str_date} {str_nom} fichiers doublons.csv",
                "w",
                newline="",
                encoding="cp1252",
            ) as f:
                w = writer(f, delimiter=";")
                w.writerow(["poids total (Ko)", "nb", "chemins"])
                for _, paths in self._hash_map.items():
                    if len(paths) > 1:  # doublons seulement
                        paths_norm = [normalize("NFC", k) for k in paths]
                        size = getsize(paths[0]) * len(paths)
                        w.writerow(
                            [
                                f"{size/1024:.1f}".replace(".", ","),
                                len(paths),
                                *paths_norm,
                            ]
                        )

    def main(self):
        """Fonction principale de la classe"""
        # on fait un scan naïf sur le 1er niveau pour jauger du niveau d'avancement
        try:
            for entry in scandir(self._chemin_in):
                if entry.is_dir(follow_symlinks=False):
                    logging.info(entry.path)
                    self._scanne(Path(entry))
                elif entry.is_file(follow_symlinks=False):
                    # pas de fichier autorisé au niveau 1
                    self._mauvais_nom.append((1, entry.path))
        except PermissionError:
            logging.critical("pas la permission de scanner %s" % self._chemin_in)
        except FileNotFoundError:
            # bug: si le chemin est trop long, le fichier/dossier n'est pas lisible
            logging.critical(
                "erreur windows, impossible de scanner %s" % self._chemin_in
            )
        # export des résultats
        self._exporte_csv()
        logging.info("vérification effectuée")


if __name__ == "__main__":
    msg = r""" 
     ___             _    _             _  _      
    / __| ___  _ _  | |_ (_) _ _   ___ | || | ___ 
    \__ \/ -_)| ' \ |  _|| || ' \ / -_)| || |/ -_)
    |___/\___||_||_| \__||_||_||_|\___||_||_|\___|
                      Vérificateur ARBOMUT
    """
    print(msg)
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser._optionals.title = "Argument à fournir"
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Affiche l'aide",
    )
    parser.add_argument(
        "-v",
        dest="verbose",
        action="count",
        default=0,
        help="la verbosité du programme (de 0 à 1)",
    )
    parser.add_argument(
        "-i",
        dest="chemin_in",
        action="store",
        required=True,
        help="le chemin vers le dossier racine à analyser",
    )
    parser.add_argument(
        "-o",
        dest="chemin_out",
        action="store",
        required=True,
        help="le chemin vers le dossier d'export du rapport",
    )
    parser.add_argument(
        "-p",
        dest="profondeur",
        action="store",
        default=3,
        help="la profondeur maximale a scanner dans l'arborescence",
    )
    commandes = parser.parse_args()

    # configuration des logs
    if commandes.verbose == 0:
        log_level = logging.INFO
    elif commandes.verbose == 1:
        log_level = logging.DEBUG
    else:
        log_level = logging.DEBUG
    logging.basicConfig(
        level=log_level,
        stream=stdout,
        format="%(levelname)s - %(message)s",
        # force=True,
    )

    # fonction principale
    Sentinelle(
        chemin_in=Path(commandes.chemin_in).resolve(),
        chemin_out=Path(commandes.chemin_out).resolve(),
        profondeur_max=commandes.profondeur,
    ).main()
