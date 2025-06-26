import traceback

from script.branch.gestion_table import CreationTable

class CreationContraintes:
    """
    Gère la création des contraintes dans la base de données.
    Elle est utilisée pour ajouter les contraintes
    (clés primaires et étrangères) aux tables créées dans la base de données.
    """
    def __init__(self, db_connection, livre):
        self.db = db_connection
        self.livre = livre

    @staticmethod
    def recup_contraintes(con) -> list:
        cursor = con.cursor()
        cursor.execute("""
        SELECT conname FROM pg_constraint
        """)
        contraintes = [contrainte[0] for contrainte in cursor.fetchall()] # .fetchall() renvoie une liste de tuple
        return contraintes


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
            return True
        return False


    def ajout_contraintes_secondaires(self, schema: str, table: str, contraintes: list):
        """
        Ajoute des contraintes de clés étrangères à une table, si elles n'existent pas déjà.
        """
        contrainte_ajoutee = False
        # Récupère les clés étrangères associées à la table, si elle est la table principale du livre
        cles_etrangeres = self.livre.relations['etrangere']

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
                contrainte_ajoutee = True
                with self.db.cursor() as cur:
                    cur.execute(contrainte_secondaire)
                    contraintes.append(nom_contrainte_secondaire)  # Ajoute la contrainte à la liste des contraintes existantes
        return contrainte_ajoutee

    def ajout_contraintes(self, tables_creees: list[str]):
        """
        Ajoute les contraintes (clés primaires et étrangères) aux tables créées.
        """
        schema = self.livre.schema

        try:
            contraintes_existantes = self.recup_contraintes(self.db)

            for table in tables_creees:
                if CreationTable.table_exist(self.db, schema, table):
                    if self.ajout_contraintes_primaires(schema, table, contraintes_existantes):
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
                if self.ajout_contraintes_secondaires(schema, nom_table_principale, contraintes_existantes):
                    print(f"Contraintes secondaires ajoutées pour {nom_table_principale}.")
            
            self.db.commit()
        except Exception as e:
            # clear_output(wait=True)
            print(f"Erreur lors de l'ajout des contraintes secondaires : {e}")
            traceback.print_exc()
            self.db.rollback()
