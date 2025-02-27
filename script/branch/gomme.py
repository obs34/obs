from IPython.display import clear_output
import time
from ..leaf.futile import *

class Gomme:
    """
    Si id_versement_cible est renseigné, supprime seulement les entrées correspondantes à cet id_versement dans la table
    et dans le dictionnaire des versements.
    Sinon, supprime une table et les entrées correspondantes dans la table de dictionnaire des versements.
    """
    def __init__(self, conn, livre):
        self.conn = conn
        self.livre = livre

        self.schema = self.livre.schema

        self.nom_table_vers = self.livre.nom_table_vers
        self.nom_table_var = self.livre.nom_table_var
        self.nom_table_mod = self.livre.nom_table_mod

        self.colnames_vers = self.livre.colnames_vers
        self.colnames_var = self.livre.colnames_var
        self.colnames_mod = self.livre.colnames_mod
        self.colnames_val = self.livre.colnames_val
        
    def get_table_list(self):
        """Récupère la liste des tables du schéma."""
        query = f"SELECT DISTINCT {self.colnames_vers[1]} FROM {self.schema}.{self.nom_table_vers}"
        with self.conn.cursor() as cur:
            cur.execute(query)
            return [x[0] for x in cur.fetchall()]
        
    def get_id_versement_list(self, table_name):
        """Récupère la liste des id_versement d'une table'."""
        query = f"SELECT DISTINCT {self.colnames_vers[0]} FROM {self.schema}.{table_name}"
        with self.conn.cursor() as cur:
            cur.execute(query)
            return [x[0] for x in cur.fetchall()]

    def get_var_mod_ids(self, table_name, id_versement:int=None):
        """Récupère les IDs des variables et modalités pour une table donnée."""
        query = f"""
        SELECT DISTINCT {self.nom_table_var}.{self.colnames_var[0]},
                       {self.nom_table_mod}.{self.colnames_mod[0]}
        FROM {self.schema}.{table_name}
        JOIN {self.nom_table_var} ON {table_name}.{self.colnames_var[0]} = {self.nom_table_var}.{self.colnames_var[0]}
        JOIN {self.nom_table_mod} ON {table_name}.{self.colnames_mod[0]} = {self.nom_table_mod}.{self.colnames_mod[0]}
        """
        if id_versement:
            query += f"WHERE {table_name}.{self.colnames_val[1]} = {id_versement}"
            
        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            return (
                list(set(x[0] for x in results)),  # var_ids
                list(set(x[1] for x in results))   # mod_ids
            )

    def build_deletion_queries(self, table_name, id_versement=None, garder_table=False):
        """Construit les requêtes de suppression nécessaires."""
        if id_versement:
            table_query = f"DELETE FROM {self.schema}.{table_name} WHERE {self.colnames_val[1]} = {id_versement}"
            vers_query = f"DELETE FROM {self.nom_table_vers} WHERE {self.colnames_vers[0]} = {id_versement}"
        else:
            table_query = f"DELETE FROM {self.schema}.{table_name}" if garder_table else f"DROP TABLE {self.schema}.{table_name}"
            vers_query = f"DELETE FROM {self.nom_table_vers} WHERE {self.colnames_vers[1]} = '{table_name}'"
            
        return table_query, vers_query

    def execute_transaction(self, queries):
        """Exécute une liste de requêtes dans une transaction."""
        try:
            if self.conn.autocommit:
                self.conn.autocommit = False
            with self.conn.cursor() as cur:
                for query in queries:
                    print(f"Exécution: {query}")
                    cur.execute(query)
            self.conn.commit()
            print("Opération réussie.")
        except Exception as error:
            self.conn.rollback()
            print(f"Erreur lors de la suppression: {error}")
        finally:
            self.conn.autocommit = True

    def suppression_table(self, table_cible, id_versement_cible=None, garder_table=False):
        """
        Supprime une table ou des entrées spécifiques d'une table et les références associées.
        
        Args:
            table_cible (str): Nom de la table à supprimer
            id_versement_cible (int, optional): ID du versement à supprimer
            garder_table (bool): Si True, garde la structure de la table
        """
        # 1. Récupérer les IDs à supprimer
        var_ids_to_delete, mod_ids_to_delete = self.get_var_mod_ids(table_cible, id_versement_cible)
        
        # 2. Vérifier les dépendances dans les autres tables
        other_tables = [t for t in self.get_table_list() if t != table_cible]
        for table in other_tables:
            other_var_ids, other_mod_ids = self.get_var_mod_ids(table)
            var_ids_to_delete = [v for v in var_ids_to_delete if v not in other_var_ids]
            mod_ids_to_delete = [m for m in mod_ids_to_delete if m not in other_mod_ids]

        # 2 bis. Vérifier les dépendances dans les autres versements de la table cible
        other_id_versements = [i for i in self.get_id_versement_list(table_cible) if i != id_versement_cible]
        for id_versement in other_id_versements:
            other_var_ids, other_mod_ids = self.get_var_mod_ids(table_cible, id_versement)
            var_ids_to_delete = [v for v in var_ids_to_delete if v not in other_var_ids]
            mod_ids_to_delete = [m for m in mod_ids_to_delete if m not in other_mod_ids]

        # 3. Construire les requêtes
        table_query, vers_query = self.build_deletion_queries(table_cible, id_versement_cible, garder_table)
        queries = [table_query, vers_query]
        
        # Ajouter les requêtes de suppression des variables et modalités si nécessaire
        if var_ids_to_delete:
            queries.append(f"DELETE FROM {self.nom_table_var} WHERE {self.colnames_var[0]} IN ({','.join(map(str, var_ids_to_delete))})")
        if mod_ids_to_delete:
            queries.append(f"DELETE FROM {self.nom_table_mod} WHERE {self.colnames_mod[0]} IN ({','.join(map(str, mod_ids_to_delete))})")

        # 4. Demander confirmation et exécuter
        str_queries = ''
        for query in queries:
            str_queries += '\n'
            str_queries += query
        print(f"Liste des requêtes:\n{str_queries}", flush=True) # flush=True force l'affichage immédiat de la sortie
        time.sleep(1)
        confirmation = demander_choix_binaire(f"Souhaitez-vous supprimer la table/versement ?\n")
        if confirmation:
            self.execute_transaction(queries)
        else:
            clear_output()
