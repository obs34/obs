"""Module de versement des données."""
import glob
import os
import pandas as pd
import psycopg2
import traceback
from typing import Dict, Any
from ..branch.lecteur_excel_csv import LecteurExcelCsv
from ..branch.gestion_table import CreationTable
from ..leaf.futile import *

class Versement:
    """Verse les données."""
    
    def __init__(self, db_connection, livre):
        """
        Initialise le verseur de données.
        
        Args:
            db_connection: Connexion à la base de données
            schema: Nom du schéma
        """
        self.db = db_connection
        self.livre = livre
        # Prend le dernier dossier de variables créé
        self.dossier_temporaire = sorted(glob.glob(f'{self.livre.PREFIXE_DOSSIER_TEMPORAIRE}*'), key=os.path.getmtime)[-1]
        self.lecteur = LecteurExcelCsv()
        self.createur_de_table = CreationTable(self.db, self.livre)

    def save_to_database(self, df: pd.DataFrame, nom_table: str):
        """
        Données : fichierCsv est un fichier CSV dont on veut reproduire la structure sur une self.base de données
                con = une instance de la fonction connexion
                table = nom (en string) de la table où vont être insérées les données
                
        résultat : insere les données de fichierCSV dans la table "table", dans une self.base de données. 
                Affiche une phrase indiquant si les données ont été insérées ou pas
        remarques : cette fonction est dédiée au versement des tables modalités, variables (et "posséder" si elle est créée)
        """
        # if table == 'valeur':
        #     # Sinon problème avec double précision
        #     values = [tuple(row[:7]) + ((None,) if row[7] == 'NULL' else (row[7],)) for row in reader] # Créer une liste de tuples à partir du fichier CSV
        # else:
        cur = self.db.cursor()
        # values = [ligne for ligne in df.itertuples(index=False, name=None)]
        # Pour que pgAdmin reconnaisse que ce sont dans valeurs NULL
        values = [
            tuple(None if pd.isna(val) else val for val in ligne)
            for ligne in df.itertuples(index=False, name=None)
        ]
        if values: # Si la table est non vide.
            # Créer des valeurs dynamiques pour la requête INSERT
            placeholder = f"({','.join(['%s'] * len(values[0]))})"
            args_str = b','.join(cur.mogrify(placeholder, row) for row in values)
            cur.execute(f"INSERT INTO {nom_table} VALUES {args_str.decode()}")

    @combien_de_temps
    def versement(self):
        """
        Verse les données dans la base.
        """
        schema = self.livre.schema
        if self.db.autocommit:
            self.db.autocommit = False # Désactiver l'autocommit pour utiliser une transaction, permet de ne rien lancer s'il y a la moindre erreur.

        # Création des tables et insertion des données
        try:
            # Met le chemin du csv de la table valeur en dernier
            # Pour éviter les conflits de clés étrangères quand on ajoute des données dans la table valeur.
            liste_csv = glob.glob(f"{self.dossier_temporaire}/*.csv")
            liste_csv.append(liste_csv.pop(next(i for i, e in enumerate(liste_csv) if self.livre.nom_table in e)))
            for csv in liste_csv:
                # Récupère les données
                # os.path.basename(csv).split('.')[0] renvoie le nom du fichier sans l'extension
                nom_table = os.path.basename(csv).split('.')[0]
                df = self.lecteur.read_csv(csv)
                # Vérifie si la table existe
                if not CreationTable.table_exist(self.db, schema, nom_table):
                    # Création de la table
                    self.createur_de_table.create_table(df, schema, nom_table)
                    # self.createur_de_table.ajout_relations(schema, nom_table)
                    print(f"Table {nom_table} créée.")
                # Sauvegarde en base
                self.save_to_database(df, nom_table)
                print(f"Données de {nom_table} insérées.")
            self.db.commit()
        except Exception as e:
            print(f"Erreur lors du versement des données : {e}")
            traceback.print_exc()
            self.db.rollback()
        
        contraintes = CreationTable.recup_contraintes(self.db)
        # Ajout des clés primaires
        try:
            for csv in glob.glob(f"{self.dossier_temporaire}/*.csv"):
                # Récupère les données
                # os.path.basename(csv).split('.')[0] renvoie le nom du fichier sans l'extension
                nom_table = os.path.basename(csv).split('.')[0]
                if not CreationTable.table_exist(self.db, schema, nom_table):
                    # Création de la table
                    raise NameError
                # Liste des contraintes
                # liste_contraintes = CreationTable.contraintes(schema, nom_table)
                self.createur_de_table.ajout_contraintes_primaires(schema, nom_table, contraintes)
                print(f"Contrainte primaire de {nom_table} ajoutée.")
            self.db.commit()
        except Exception as e:
            print(f"Erreur lors de l'ajout des contraintes primaires : {e}")
            traceback.print_exc()
            self.db.rollback()

        # Ajout des clés secondaires
        try:
            # for csv in glob.glob(f"{self.dossier_temporaire}/*.csv"):
            #     # Récupère les données
            #     # os.path.basename(csv).split('.')[0] renvoie le nom du fichier sans l'extension
            #     nom_table = os.path.basename(csv).split('.')[0]
            if not CreationTable.table_exist(self.db, schema, nom_table):
                # Création de la table
                raise NameError
            nom_table = self.livre.nom_table
            self.createur_de_table.ajout_contraintes_secondaires(schema, nom_table, contraintes)
            print(f"Contraintes secondaires de {nom_table} ajoutées.")
            self.db.commit()
        except Exception as e:
            print(f"Erreur lors de l'ajout des contraintes secondaires : {e}")
            traceback.print_exc()
            self.db.rollback()

        # try:
        #     nom_table = self.livre.nom_table
        #     if not CreationTable.table_exist(self.db, schema, nom_table):
        #         # Création de la table
        #         raise NameError
        #     self.createur_de_table.ajout_contraintes(schema, nom_table)
        #     print(f"Contraintes de {nom_table} ajoutées.")
        #     self.db.commit()
        # except Exception as e:
        #     print(f"Erreur lors de l'ajout des contraintes : {e}")
        #     traceback.print_exc()
        #     self.db.rollback()

        # finally:
        #     self.db.close()
            # Supprime le dossier de variables
            # shutil.rmtree(self.nom_dossier_vars)


