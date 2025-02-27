"""Module de versement des données."""
import glob
import os
import psycopg2
import pandas as pd
import traceback
from IPython.display import clear_output
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
            livre: Objet contenant les informations nécessaires
        """
        self.db = db_connection
        self.livre = livre
        # Prend le dernier dossier de variables créé
        self.dossier_temporaire = sorted(glob.glob(f'{self.livre.PREFIXE_DOSSIER_TEMPORAIRE}*'), key=os.path.getmtime)[-1]
        self.lecteur = LecteurExcelCsv()
        self.createur_de_table = CreationTable(self.db, self.livre)

    def save_to_database(self, df: pd.DataFrame, nom_table: str, ignore=False):
        """
        Insère les données du DataFrame dans la table spécifiée.
        """
        # if df.empty:
        #     print(f"Le DataFrame est vide pour {nom_table}.")
        #     return
        
        cur = self.db.cursor()

        values = [tuple(None if pd.isna(val) else val for val in ligne) for ligne in df.itertuples(index=False, name=None)] # df.itertuples(index=False, name=None) pour que pgAdmin reconnaisse que ce sont dans valeurs NULL
        placeholder = f"({','.join(['%s'] * len(values[0]))})"
        args_str = b','.join(cur.mogrify(placeholder, row) for row in values) # b pour bytes (octets) à la place de str

        fin = 'ON CONFLICT DO NOTHING' if ignore else ''
        requete = f"INSERT INTO {nom_table} VALUES {args_str.decode()} {fin};"

        try:
            cur.execute("SAVEPOINT before_insert")  # Crée un point de sauvegarde
            cur.execute(requete)
        except psycopg2.errors.UniqueViolation:
            # print(f"Erreur : Violation de clé primaire détectée dans {nom_table} : {e}")
            # self.db.rollback()  # IMPORTANT : Annule toute la transaction avant de demander quoi faire
            # raise  # Propage l'erreur pour qu'elle soit capturée ailleurs
            cur.execute("ROLLBACK TO SAVEPOINT before_insert")  # Annule seulement cette insertion
            return "doubons"  # Indique un problème
        except Exception as e:
            # clear_output(wait=True)
            print(f"Erreur lors de l'insertion dans {nom_table} : {e}")
            traceback.print_exc()
            self.db.rollback()
            return False  # Indique un problème
        return True  # Indique que tout s'est bien passé

    def ajout_contraintes(self, schema, tables):
        """
        Ajoute les contraintes (clés primaires et étrangères) aux tables créées.
        """
        try:
            contraintes_existantes = CreationTable.recup_contraintes(self.db)

            for table in tables:
                if CreationTable.table_exist(self.db, schema, table):
                    if self.createur_de_table.ajout_contraintes_primaires(schema, table, contraintes_existantes):
                        print(f"Contrainte primaire ajoutée pour {table}.")
            
            self.db.commit()
        except Exception as e:
            # clear_output(wait=True)
            print(f"Erreur lors de l'ajout des contraintes primaires : {e}")
            traceback.print_exc()
            self.db.rollback()

        try:
            nom_table_principale = self.livre.nom_table
            if CreationTable.table_exist(self.db, schema, nom_table_principale):
                if self.createur_de_table.ajout_contraintes_secondaires(schema, nom_table_principale, contraintes_existantes):
                    print(f"Contraintes secondaires ajoutées pour {nom_table_principale}.")
            
            self.db.commit()
        except Exception as e:
            # clear_output(wait=True)
            print(f"Erreur lors de l'ajout des contraintes secondaires : {e}")
            traceback.print_exc()
            self.db.rollback()

    @combien_de_temps
    def versement(self):
        """
        Verse les données dans la base.
        """
        schema = self.livre.schema
        if self.db.autocommit:
            self.db.autocommit = False # Désactiver l'autocommit pour utiliser une transaction, permet de ne rien lancer s'il y a la moindre erreur.

        try:
            # Pour éviter les conflits de clés étrangères quand on ajoute des données dans la table valeur.
            # Liste et trie les fichiers CSV
            fichiers_csv = glob.glob(f"{self.dossier_temporaire}/*.csv")
            fichiers_csv.sort(key=lambda x: self.livre.nom_table in x, reverse=False)  # Place la table principale à la fin

            tables_creees = [] # Pour ajouter les contraintes

            for csv in fichiers_csv:
                # Récupère les données
                nom_table = os.path.basename(csv).split('.')[0] # renvoie le nom du fichier sans l'extension
                df = self.lecteur.read_csv(csv, sep=self.livre.sep, encoding=self.livre.encoding)

                # Vérifie si la table existe
                if not CreationTable.table_exist(self.db, schema, nom_table):
                    # Création de la table
                    self.createur_de_table.create_table(df, schema, nom_table)
                    tables_creees.append(nom_table)
                    print(f"Table {nom_table} créée.")

                try:
                    # Insertion des données
                    if not df.empty:
                        succes = self.save_to_database(df, nom_table)
                        if succes is True:
                            print(f"Données insérées dans {nom_table}.")
                        if succes=="doubons":  # Si une erreur a été détectée
                            print(f"Il y a des doublons dans {nom_table}.", flush=True)
                            choix = demander_choix_binaire("Voulez-vous ignorer les doublons et continuer ?")

                            if choix:
                                self.save_to_database(df, nom_table, ignore=True)
                                print(f"Données insérées dans {nom_table} en ignorant les doublons.")
                            else:
                                print("Toutes les transactions sont annulées.", flush=True)
                                self.db.rollback()
                                return  # Stopper l'exécution
                    else:
                        continue
                except Exception as e:
                    # clear_output(wait=True)
                    print(f"Erreur critique lors du versement dans {nom_table} : {e}")
                    self.db.rollback()  # On rollback seulement si c'est une erreur majeure
                    return  # Stopper l'exécution

            self.db.commit()  # Valide les changements uniquement si tout s'est bien passé

        except Exception as e:
            # clear_output(wait=True)
            print(f"Erreur globale lors du versement des données : {e}")
            traceback.print_exc()
            self.db.rollback()
        
        # Ajout des contraintes après insertion
        self.ajout_contraintes(schema, tables_creees)
