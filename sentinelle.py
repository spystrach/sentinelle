#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Vérification de la conformité d'un serveur de fichier ARBOMUT"""


import logging
import argparse
from os import scandir, cpu_count
from pathlib import Path
from sys import stdout
import hashlib
from csv import writer
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from re import compile
from datetime import datetime

LONGUEUR_MAXIMALE_FICHIER = 255
REGEX_DOSSIER_VIDE = compile(r"^[\w ]+-VIDE$")
REGEX_NIVEAU_1 = compile(r"^([A-Z])\w+$")
REGEX_NIVEAU_2 = compile(r"^([A-Z])\w+$")
REGEX_NIVEAU_3 = compile(r"^([A-Z])\w+$")


class SentinelleErreur(Exception):
    pass


class Sentinelle:

    def __init__(self, chemin_in: Path, chemin_out: Path) -> None:
        """Initialisation de l'objet"""
        self._chemin_in = chemin_in
        self._niveau_chemin_in = len(chemin_in.parents)
        self._chemin_out = chemin_out
        # vérification des chemins
        self._verifie_chemins()
        # variables
        self._now = datetime.now()
        self._trop_long = []
        self._mauvais_nom = []
        self._vide = []
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
                while chunk := f.read(8192):
                    hasher.update(chunk)
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
        if niveau == 1:
            if not REGEX_NIVEAU_1.match(dossier.name):
                self._mauvais_nom.append((niveau, str(dossier)))
        elif niveau == 2:
            if not REGEX_NIVEAU_2.match(dossier.name):
                self._mauvais_nom.append((niveau, str(dossier)))
        elif niveau == 3:
            if not REGEX_NIVEAU_3.match(dossier.name):
                self._mauvais_nom.append((niveau, str(dossier)))

    @staticmethod
    def _is_dossier_non_vide(dossier):
        """vérifie qu'un dossier est vide tout fichier"""
        stack = [dossier]
        while stack:
            current = stack.pop(0)
            try:
                with scandir(current) as it:
                    for entry in it:
                        if entry.is_file(follow_symlinks=False):
                            return True  # Fichier trouvé, on arrête tout
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
            except PermissionError:
                pass
        return False  # Aucun fichier trouvé dans ce dossier ni ses sous-dossiers

    def _verif_dossier_vide(self, dossier: Path):
        """vérifie qu'un dossier se prétendant vide l'est bien"""
        if REGEX_DOSSIER_VIDE.match(str(dossier.name)):
            print("VIDE ??")
            if self._is_dossier_non_vide(dossier):
                self._vide.append(dossier)

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
                        # logging pour les dossiers de premier niveau
                        if entry_path.parent == self._chemin_in and entry.is_dir(
                            follow_symlinks=False
                        ):
                            logging.info(entry.path)

                        # si le scanné est un dossier, on l'ajoute à la pile des dossiers à scanner
                        if entry.is_dir(follow_symlinks=False):
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

        # Détermination des doublons (attente de tous les hachages si besoin)
        for future in as_completed(futures):
            hash_result, path = future.result()
            if hash_result:
                self._hash_map[hash_result].append(path)
        self._dupliques = {
            h: paths for h, paths in self._hash_map.items() if len(paths) > 1
        }

    def _exporte_csv(self):
        """exporte les résultats dans des fichiers CSV"""
        # Export dossiers avec noms invalides
        str_date = self._now.strftime("%Y-%m-%d %H%M%S")
        if self._mauvais_nom:
            with open(
                self._chemin_out / f"{str_date} dossiers_mal_nommés.csv",
                "w",
                newline="",
                encoding="utf-8",
            ) as f:
                w = writer(f)
                w.writerow(["niveau", "chemin"])
                for niv, path in self._mauvais_nom:
                    w.writerow([niv, path])

        # Export dossiers non vides (qui se font passer pour vide)
        if self._vide:
            with open(
                self._chemin_out / f"{str_date} dossiers_non_vides.csv",
                "w",
                newline="",
                encoding="utf-8",
            ) as f:
                w = writer(f)
                w.writerow(["chemin"])
                for path in self._vide:
                    w.writerow([path])

        # Export fichiers trop longs
        if self._trop_long:
            with open(
                self._chemin_out / f"{str_date} fichiers_trop_longs.csv",
                "w",
                newline="",
                encoding="utf-8",
            ) as f:
                w = writer(f)
                w.writerow(["chemin"])
                for path in self._trop_long:
                    w.writerow([path])

        # Export doublons (hash -> liste des fichiers)
        if any(len(paths) > 1 for paths in self._hash_map.values()):
            with open(
                self._chemin_out / f"{str_date} fichiers_doublons.csv",
                "w",
                newline="",
                encoding="utf-8",
            ) as f:
                w = writer(f)
                w.writerow(["hash", "chemins"])
                for hash_value, paths in self._hash_map.items():
                    if len(paths) > 1:  # doublons seulement
                        w.writerow([hash_value, ";".join(paths)])

    def main(self):
        """Fonction principale de la classe"""
        self._scanne(self._chemin_in)
        self._exporte_csv()


if __name__ == "__main__":
    msg = r""" 
     ___             _    _             _  _      
    / __| ___  _ _  | |_ (_) _ _   ___ | || | ___ 
    \__ \/ -_)| ' \ |  _|| || ' \ / -_)| || |/ -_)
    |___/\___||_||_| \__||_||_||_|\___||_||_|\___|
                      Vérificateur ARBOMUT
    """
    print(msg)
    parser = argparse.ArgumentParser(description=__doc__)
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
        force=True,
    )

    # fonction principale
    Sentinelle(
        chemin_in=Path(commandes.chemin_in).resolve(),
        chemin_out=Path(commandes.chemin_out).resolve(),
    ).main()
