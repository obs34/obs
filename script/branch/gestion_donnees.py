import glob
import os
import pandas as pd
import psycopg2
import traceback
from typing import Dict

from script.leaf.futile import *

class GestionDonnees():
    """
    Gère l'insertion des données dans la base de données.
    Et l'ajout des contraintes.
    Elle est seulement utilisée pour l'insertion des données dans la base de données 
    dans la deuxième partie du script : "versement".
    """
    def __init__(self, db_connection, livre):
        self.db = db_connection
        self.livre = livre

    def lire_feuilles(self, file_path) -> Dict[str, pd.DataFrame]:
        """
        Lit toutes les feuilles du fichier Excel.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des feuilles Excel
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            sheets = {}
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                # Nettoie les noms de colonnes
                # df.columns = self._clean_column_names(df.columns)
                sheets[sheet_name] = df
            return sheets
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier Excel : {e}")
            raise

    def lire_donnees(self, dossier_temporaire: str) -> Dict[str, pd.DataFrame]:
        """
        Lit un fichier CSV.
        
        Args:
            sep: Séparateur de colonnes
        
        Returns:
            pd.DataFrame: Données du fichier CSV
        """
        fichiers_csv = glob.glob(f"{dossier_temporaire}/*.csv")
        fichiers_csv.sort(key=lambda x: self.livre.nom_table in x, reverse=False)

        try:
            # nom : os.path.basename(fichier).split('.')[0]
            # df : pd.read_csv(fichier, sep=self.livre.sep, encoding=self.livre.encoding)
            return {os.path.basename(fichier).split('.')[0]: pd.read_csv(fichier, sep=self.livre.sep, encoding=self.livre.encoding)
                    for fichier in fichiers_csv}
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier CSV : {e}")
            raise

    @staticmethod
    def code_echelle_auto(code_entite: str) -> str:
        """
        Retourne le code d'échelle automatique en fonction du code d'entité.
        
        Args:
            code_entite (str): Code de l'entité
        
        Returns:
            str: Code d'échelle automatique
        """
        if not isinstance(int(code_entite), int):
            raise ValueError(f"Le code_entite '{code_entite}' n'est pas un entier valide. L'échelle automatique ne peut pas être déterminée.")
        code_entite = str(code_entite).strip()
        if len(code_entite) == 5:
            return 'commune'
        if len(code_entite) == 2:
            return 'departement'
        if len(code_entite) == 9 and not code_entite.startswith('34'):
            return 'epci'
        if len(code_entite) == 9 and code_entite.startswith('34'):
            return 'iris'
        else:
            raise ValueError(f"Le code_entite '{code_entite}' n'est pas reconnu pour l'échelle automatique. Veuillez vérifier le format du code.")
    
    def inserer_donnees(self, df: pd.DataFrame, nom_table: str, ignore=False):
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

    def insertion_donnees(self, fichiers_csv: Dict[str, pd.DataFrame]) -> None:
        """
        Intègre les données des fichiers CSV dans la base de données.
        
        Args:
            fichiers_csv (list[str]): Liste des chemins des fichiers CSV à intégrer.
        
        Returns:
            None
        """
        try:
            for nom_table, df in fichiers_csv.items():
            # Insertion des données
                if not df.empty:
                    succes = self.inserer_donnees(df, nom_table)
                    if succes is True:
                        print(f"Données insérées dans {nom_table}.")
                    if succes=="doubons":  # Si une erreur a été détectée
                        print(f"Il y a des doublons dans {nom_table}.", flush=True)
                        choix = demander_choix_binaire("Voulez-vous ignorer les doublons et continuer ?")

                        if choix:
                            self.inserer_donnees(df, nom_table, ignore=True)
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
            raise  # Propage l'erreur pour qu'elle soit capturée ailleurs
