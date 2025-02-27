#Ce fichier gère le versement des données dans PostgreSQL.
import pandas as pd
import psycopg2

class Versement:
    def __init__(self, connection, schema):
        self.conn = connection
        self.schema = schema

    def inserer_donnees(self, df, table_name):
        """Insère un DataFrame dans PostgreSQL."""
        try:
            cursor = self.conn.cursor()
            for _, row in df.iterrows():
                query = f"INSERT INTO {self.schema}.{table_name} VALUES {tuple(row)}"
                cursor.execute(query)
            self.conn.commit()
            return f"{len(df)} lignes insérées dans {table_name}"
        except psycopg2.Error as e:
            return f"Erreur PostgreSQL : {e}"
        finally:
            cursor.close()

    def fermer_connexion(self):
        self.conn.close()
