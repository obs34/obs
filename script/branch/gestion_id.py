import pandas as pd
import numpy as np
from typing import Dict, Any, Union

from .gestion_table import CreationTable

class GestionId:

    OBJECTID = 'objectid'

    def __init__(self, db_connection, livre):
        self.db = db_connection
        self.livre = livre

    def _execute_query(self, query: str) -> list:
        """Exécute une requête SQL et retourne les résultats."""
        with self.db.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def id_nom_present_pg(self, col_id: str, col_nom: str, nom_table: str) -> list[tuple[int, str]]:
        if CreationTable.table_exist(self.db, self.livre.schema, nom_table):
            query = f"SELECT {col_id}, {col_nom} FROM {self.livre.schema}.{nom_table}"
            return self._execute_query(query)
        return []

    def id_nom_present_processed_data(self, processed_data: Dict[str, pd.DataFrame], col_id: str, col_nom: str, nom_table: str) -> list[tuple[int, str]]:
        return [(row[1], row[2]) for row in processed_data[nom_table][[col_id, col_nom]].itertuples()]
    
    # https://stackoverflow.com/questions/36909977/update-row-values-where-certain-condition-is-met-in-pandas   
    def ajout_id_present(self, id_nom_present: list[tuple[int, str]], processed_data: Dict[str, pd.DataFrame], nom_table: str, col_id: str, col_nom: str):
        if id_nom_present:
            df = processed_data[nom_table]
            for id, nom in id_nom_present:
                df.loc[df[col_nom] == nom, col_id] = id
        return processed_data

    def suppression_id_present(self, id_nom_present: list[tuple[int, str]], processed_data: Dict[str, pd.DataFrame], nom_table: str, col_id: str):
        df = processed_data[nom_table]
        if id_nom_present:
            # Extraction des indices des lignes à supprimer
            id_nom_present_idx = df[df[col_id].isin([tup[0] for tup in id_nom_present])].index
            # Suppression des lignes correspondantes
            df.drop(id_nom_present_idx, inplace=True)
        return processed_data

    def select_max_id(self, col_id: str, nom_table: str) -> int:
        if CreationTable.table_exist(self.db, self.livre.schema, nom_table):
            query = f"SELECT max({col_id}) FROM {nom_table}"
            result = self._execute_query(query)
            return result[0][0] if result else 0
        return 0

    def ajout_id_manquant(self, max_id: int, processed_data: Dict[str, pd.DataFrame], nom_table: str, col_id: str):
        df = processed_data[nom_table]
        # df[col_id] = df[col_id].fillna(lambda: max_id + 1).astype(int)
        for row in range(len(df[col_id])):
            if df.loc[row, col_id] is None:
                df.loc[row, col_id] = max_id + 1
                max_id += 1 
        df[col_id] = df[col_id].astype(int)

        return processed_data

    def ajout_id_composite(self, processed_data: Dict[str, pd.DataFrame]):
        df = processed_data[self.livre.nom_table]
        df[self.livre.colnames_val[0]] = df[self.livre.colnames_val[2:7]].astype(str).sum(axis=1)
        return processed_data

    def trouve_id_versement(self) -> int:
        if CreationTable.table_exist(self.db, self.livre.schema, self.livre.nom_table_vers):
            query = f"SELECT max({self.livre.colnames_vers[0]}) FROM {self.livre.nom_table_vers}"
            result = self._execute_query(query)
            return (result[0][0] or 0) + 1
        return 1

    def ajout_id_versement(self, id_versement: int, processed_data: Dict[str, pd.DataFrame], nom_table: str, col_id: str):
        processed_data[nom_table][col_id] = id_versement
        return processed_data

    def _traiter_table(self, processed_data: Dict[str, pd.DataFrame], col_id: str, col_nom: str, nom_table: str):
        id_nom_pg = self.id_nom_present_pg(col_id, col_nom, nom_table)
        processed_data = self.ajout_id_present(id_nom_pg, processed_data, nom_table, col_id, col_nom)
        max_id = self.select_max_id(col_id, nom_table)
        return self.ajout_id_manquant(max_id, processed_data, nom_table, col_id), id_nom_pg

    def presence_objectid(self, nom_table: str) -> Union[int,bool]:
        """
        Vérifie si la colonne 'objectid' est présente dans la table.
        Retourne l'index de 'objectid' si il est présent, False sinon.
        """
        if CreationTable.table_exist(self.db, self.livre.schema, nom_table):
                query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{nom_table}'"
                resultat = [tup[0] for tup in self._execute_query(query)]
                presence = resultat.index(self.OBJECTID) if self.OBJECTID in resultat else False
                return presence
        else:
            # Si la table n'existe pas, retourne False
            return False

    def max_objectid(self, nom_table: str) -> tuple[int,int]:
        """
        Retourne la valeur maximale de la colonne 'objectid' si elle existe,
        sinon lève une exception ou retourne None.
        """
        presence = self.presence_objectid(nom_table)
        if presence:
            try:
                query = f"SELECT max({self.OBJECTID}) FROM {self.livre.schema}.{nom_table}"
                resultat = self._execute_query(query)[0][0]
                return (presence, resultat)
            except Exception as e:
                print(f"Erreur dans max_objectid() : {e}")
                return None

    def ajout_objectid(self, object_id: tuple[int,int], processed_data: Dict[str, pd.DataFrame], nom_table: str):
        '''
        Ajoute en première colonne la colonne 'objectid' qui est un autoincrémenté en integer
        et créé par ArcGIS par le géotraitement arcpy.TableToGeodatabase_conversion().
        Elle permet à ArcGIS de reconnaître la table plus facilement pour la publication et l'inscription.
        Le +1 est là parce que np.arrange() commence à 0.
        On n'ajoute la colonne objectid que si la table dans laquelle on veut verser le Pandas a déjà cette colonne.
        '''
        df = processed_data[nom_table]
        index_objectid, max_objectid_value = object_id
        if len(df)>0: # Pour les tables mod et var, sinon ça créer une colonne 'objectid' vide.
            df[self.OBJECTID] = np.arange(df.shape[0]) + max_objectid_value + 1 # Créer la colonne 'objectid' en dernier
            # Place 'objectid en fonction de son index
            l = list(df.columns)
            l.insert(index_objectid, l.pop(l.index(self.OBJECTID))) # note : pop renvoie la valeur supprimée
            df = df[[col for col in l]]
            processed_data[nom_table] = df
        return processed_data
    
    def set_id(self, processed_data: Dict[str, pd.DataFrame]):
        """Ajoute les identifiants aux données."""
        # variables (id_var)
        processed_data, id_nom_pg_var = self._traiter_table(processed_data, self.livre.colnames_var[0], self.livre.colnames_var[1], self.livre.nom_table_var)
        
        # modalités (id_mod)
        processed_data, id_nom_pg_mod = self._traiter_table(processed_data, self.livre.colnames_mod[0], self.livre.colnames_mod[1], self.livre.nom_table_mod)

        # valeurs (id_var, id_mod)
        for col_id_val, col_nom_val, col_id_mv, col_nom_mv, nom_table_mv in [(self.livre.colnames_val[5], self.livre.colnames_val[5], 
                                                                              self.livre.colnames_var[0], self.livre.colnames_var[1], self.livre.nom_table_var),
                                                                              (self.livre.colnames_val[6], self.livre.colnames_val[6], 
                                                                               self.livre.colnames_mod[0], self.livre.colnames_mod[1], self.livre.nom_table_mod)]:
            id_nom_proc = self.id_nom_present_processed_data(processed_data, col_id_mv, col_nom_mv, nom_table_mv)
            processed_data = self.ajout_id_present(id_nom_proc, processed_data, self.livre.nom_table, col_id_val, col_nom_val)
        processed_data = self.ajout_id_composite(processed_data)

        # variables (id_var) (on supprime les id qui existent déjà dans pg pour éviter les doublons)
        processed_data = self.suppression_id_present(id_nom_pg_var, processed_data, self.livre.nom_table_var, self.livre.colnames_var[0])

        # modalités (id_mod) (on supprime les id qui existent déjà dans pg pour éviter les doublons)
        processed_data = self.suppression_id_present(id_nom_pg_mod, processed_data, self.livre.nom_table_mod, self.livre.colnames_mod[0])

        # ID versement
        id_versement = self.trouve_id_versement()
        for table, col_id in [(self.livre.nom_table, self.livre.colnames_val[1]),
                              (self.livre.nom_table_vers, self.livre.colnames_vers[0])]:
            processed_data = self.ajout_id_versement(id_versement, processed_data, table, col_id)

        # objectid
        # En faisant l'hypothèse que la colonne 'objectid' créée par ArcGIS soit tout le temps en premier et qu'elle soit nommée 'objectid' et qu'elle soit auto-incrémentée en int.
        for table, col_id in [(self.livre.nom_table, self.OBJECTID),
                              (self.livre.nom_table_vers, self.OBJECTID),
                              (self.livre.nom_table_mod, self.OBJECTID),
                              (self.livre.nom_table_var, self.OBJECTID)]:
            object_id = None
            object_id = self.max_objectid(table)
            if object_id:
                processed_data = self.ajout_objectid(object_id, processed_data, table)

        return processed_data
