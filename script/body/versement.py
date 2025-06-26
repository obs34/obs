"""Module de versement des données."""
import glob
import os
import traceback

from script.branch.lecteur_excel_csv import LecteurExcelCsv
from script.branch.gestion_table import CreationTable
from script.branch.gestion_donnees import GestionDonnees
from script.branch.gestion_contraintes import CreationContraintes
from script.leaf.futile import *

class Versement:
    """Verse les données."""
    
    def __init__(self, db_connection, livre):
        """
        Initialise le verseur de données.
        
        Args:
            db_connection: Connexion à la base de données
            livre: Objet contenant les informations nécessaires
        """
        self.db = db_connection
        self.livre = livre
        # Prend le dernier dossier de variables créé
        self.dossier_temporaire = sorted(glob.glob(f'{self.livre.PREFIXE_DOSSIER_TEMPORAIRE}*'), key=os.path.getmtime)[-1]
        self.lecteur = LecteurExcelCsv()
        self.createur_de_table = CreationTable(self.db, self.livre)
        self.gestion_donnees = GestionDonnees(self.db, self.livre)
        self.createur_de_contraintes = CreationContraintes(self.db, self.livre)

    @combien_de_temps
    def versement(self):
        """
        Verse les données dans la base.
        """       
        
        if self.db.autocommit:
            self.db.autocommit = False # Désactiver l'autocommit pour utiliser une transaction, permet de ne rien lancer s'il y a la moindre erreur.

        # Lecture des données CSV
        fichiers_csv = self.gestion_donnees.lire_donnees(self.dossier_temporaire)

        # Création des tables
        tables_creees = self.createur_de_table.creation_table(fichiers_csv)

        # Insertion des données
        self.gestion_donnees.insertion_donnees(fichiers_csv)

        # Ajout des contraintes après insertion
        self.createur_de_contraintes.ajout_contraintes(tables_creees)

        self.db.commit()