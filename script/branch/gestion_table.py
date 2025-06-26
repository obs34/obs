"""Module de création de table."""
import os
import pandas as pd
from typing import Dict


class CreationTable:
    """Créer les tables si elles n'existent pas."""

    def __init__(self, db_connection, livre):
        """
        Initialise le processeur de données.
        
        Args:
            db_connection: Connexion à la base de données
            schema: Nom du schéma
        """
        self.db = db_connection
        self.livre = livre

    @staticmethod
    def table_exist(con, nom_schema, nom_table):
        # con.autocommit = True
        # with con.cursor() as cursor:
        cursor = con.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_name = %s and table_schema = %s
            );
        """, (nom_table, nom_schema))
        table_existe = cursor.fetchone()[0]
        # cursor.close()
        return table_existe

    def creer_table(self, df: pd.DataFrame, schema: str, table: str) -> None:
        """
        Crée une table dans la base de données avec ses colonnes, clés primaires et clés étrangères.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données pour déterminer les types des colonnes.
            schema (str): Le schéma de la base de données où la table sera créée.
            table (str): Le nom de la table à créer.

        Returns:
            None
        """
        # Étape 1 : Déterminer les types des colonnes
        list_empty = [df[col].isnull().all() for col in df.columns]
        colonnes_types = [
            f"{col} {self.map_pandas_to_postgres_type(df[col].dtype, is_empty=is_empty)}"
            for col, is_empty in zip(df.columns, list_empty)
        ]

        definition_complete = ', '.join(colonnes_types)

        requete_creation = f"CREATE TABLE {schema}.{table} ({definition_complete});"

        # Étape 2 : Exécution de la requête (sans commit)
        with self.db.cursor() as cur:
            cur.execute(requete_creation)


    def map_pandas_to_postgres_type(self, dtype, is_empty=False):
        # Dictionnaire des types Pandas vers PostgreSQL
        postgres_types = {
            'int64': 'INTEGER',
            'float64': 'NUMERIC(38, 8)',
            'object': 'VARCHAR(8000)',
            'O': 'VARCHAR(8000)',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }
        # Si la colonne est entièrement vide, on retourne un type générique VARCHAR
        if is_empty:
            return 'VARCHAR(8000)'

        # Gestion des cas non mappés
        dtype_str = str(dtype)
        if dtype_str in postgres_types:
            return postgres_types[dtype_str]
        else:
            raise ValueError(f"Type Pandas non supporté : {dtype}")
        
    def creation_table(self, fichiers_csv: Dict[str, pd.DataFrame]) -> list:
        """
        Crée les tables dans la base de données si elles n'existent pas déjà.

        Args:
            nom_tables (list): Liste des noms des tables à créer.

        Returns:
            list: Liste des tables créées.
        """
        tables_creees = []
        for nom_table, df in fichiers_csv.items():
            if not self.table_exist(self.db, self.livre.schema, nom_table):
                self.creer_table(df, self.livre.schema, nom_table)
                tables_creees.append(nom_table)
                print(f"Table {nom_table} créée.")
        return tables_creees