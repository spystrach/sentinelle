# sentinelle

Vérification de la conformité d'un serveur de fichier ARBOMUT

## Usage

Ce script est écrit en python et utilise uniquement la bibliothèque standard. 
Il est compatible de *python 3.7* jusqu'aux versions les plus récentes.

### Obtenir de l'aide

Pour afficher l'ensemble des options disponibles :

```sh
python sentinelle.py -h
```

### Lancer un scan

Pour lancer un scan, il faut préciser :
- le chemin vers le dossier à scanner (donc le lecteur "N°_TrigrammeBDD");
- le chemin vers le dossier dans lequel les résultats seront générés (de préférence sur la machine qui effectue le scan).

```sh
python sentinelle.py -i P:\0_EVX -o C:\temp
```

### Options supplémentaires

Il est possible d'obtenir plus d'informations à l'écran lors de l'exécution du programme en
ajoutant l'option **-v** (plus d'information) ou **-vv** (encore plus d'informations).

Il est possible de préciser la profondeur maximale (dans les sous-dossiers) de scan. Pour cela, ajouter
l'option **-p 5** pour aller jusqu'au niveau 5 par exemple. Par défaut, sa valeur est de **3**.
