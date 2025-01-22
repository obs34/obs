"""Module de création de table."""
import pandas as pd

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

    @staticmethod
    def recup_contraintes(con) -> list:
        cursor = con.cursor()
        cursor.execute("""
SELECT conname FROM pg_constraint
        """)
        contraintes = [contrainte[0] for contrainte in cursor.fetchall()] # .fetchall() renvoie une liste de tuple
        return contraintes

    def create_table(self, df: pd.DataFrame, schema: str, table: str) -> None:
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

        definition_complete = ', '.join(colonnes_types) #  + contraintes

        requete_creation = f"CREATE TABLE {schema}.{table} ({definition_complete});"
        print(requete_creation)
        # Étape 5 : Exécution de la requête (sans commit)
        with self.db.cursor() as cur:
            cur.execute(requete_creation)

    def ajout_contraintes_primaires(self, schema: str, table: str, contraintes: list) -> None:
        """
        Ajoute une contrainte de clé primaire à une table, si elle n'existe pas déjà.
        """
        cle_primaire = self.livre.relations['primaire'][table]  # Récupère la clé primaire de la table
        nom_contrainte_primaire = f"{table}_{cle_primaire}_pk"  # Nom de la contrainte

        # Génère la commande SQL pour ajouter la clé primaire
        contrainte_primaire = (
            f"ALTER TABLE {schema}.{table} "
            f"ADD CONSTRAINT {nom_contrainte_primaire} PRIMARY KEY ({cle_primaire});"
        )

        # Vérifie si la contrainte n'a pas déjà été ajoutée
        if nom_contrainte_primaire not in contraintes:
            with self.db.cursor() as cur:
                cur.execute(contrainte_primaire)
                contraintes.append(nom_contrainte_primaire)  # Ajoute la contrainte à la liste des contraintes existantes


    def ajout_contraintes_secondaires(self, schema: str, table: str, contraintes: list):
        """
        Ajoute des contraintes de clés étrangères à une table, si elles n'existent pas déjà.
        """
        # Récupère les clés étrangères associées à la table, si elle est la table principale du livre
        cles_etrangeres = self.livre.relations['etrangere']
        print(f"cles_etrangeres:{cles_etrangeres}")
        for table_referente, (cle_etrangere_referente, cle_etrangere_maison) in cles_etrangeres.items():
            # Nom de la contrainte étrangère
            nom_contrainte_secondaire = f"{table}_{cle_etrangere_maison}_fk"

            # Génère la commande SQL pour ajouter la clé étrangère
            contrainte_secondaire = (
                f"ALTER TABLE {schema}.{table} "
                f"ADD CONSTRAINT {nom_contrainte_secondaire} "
                f"FOREIGN KEY ({cle_etrangere_maison}) "
                f"REFERENCES {schema}.{table_referente} ({cle_etrangere_referente});"
            )
            # Vérifie si la contrainte n'a pas déjà été ajoutée
            if nom_contrainte_secondaire not in contraintes:
                with self.db.cursor() as cur:
                    cur.execute(contrainte_secondaire)
                    contraintes.append(nom_contrainte_secondaire)  # Ajoute la contrainte à la liste des contraintes existantes

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