from IPython.display import display
import pandas as pd
import time

class Catalogue:

    def __init__(self, livre):
        self.livre = livre

    def catalogue(self):

        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

        # https://www.geeksforgeeks.org/how-to-print-an-entire-pandas-dataframe-in-python/

        dictionnaires = {'1': {'choix': "pour faire une recherche sur les modalités et les variables d'une table en particulier.",
                                'nom_table': None},
                        '2': {'choix': "pour faire une recherche sur la table des modalités.",
                                        'nom_table': self.livre.nom_table_mod},
                        '3': {'choix': "pour faire une recherche sur la table des variables.",
                                'nom_table': self.livre.nom_table_var},
                        '4': {'choix': "pour faire une recherche sur le dictionnaire des tables.",
                                'nom_table': self.livre.nom_table_vers}}
        print("""Liste des dictionnaires disponibles :""")
        for dictionnaire, params in dictionnaires.items():
            print(f"""{dictionnaire} : {params['choix']}""")
        time.sleep(1) # Pour éviter que les messages se mélangent
        ok=False
        while not ok:
            dictionnaire = input("""Quel dictionnaire voulez-vous ?""")
            if dictionnaire in dictionnaires:
                params = dictionnaires[dictionnaire]
                ok = True
            else:
                print("""“Choisir le doute comme philosophie de vie c’est comme choisir l’immobilité comme mode de transport.” - Yann Martel """)
        if dictionnaire=='1':
            liste_table = self.liste_table(afficher=False)
            print("""Liste des tables disponibles :""")
            m = max([len(u) for u in liste_table])
            fibonacci = [0,1,1]
            for table in liste_table:
                print(f"{table}" + ' '*(m-len(str(table))) + f" | {fibonacci[-3]}")
                fibonacci.append(fibonacci[-2]+fibonacci[-1])
            time.sleep(1) # Pour éviter que les messages se mélangent
            # plt.plot(fibonacci)
            # plt.show()
            ok = False
            while not ok:
                table = input("Sur quelle table voulez-vous travailler ?")
                if table in liste_table:
                    ok = True
                    requete_sql = f"""
                    select distinct
                    {self.livre.nom_table_var}.id_var,
                    {self.livre.nom_table_var}.nom_var,
                    {self.livre.nom_table_var}.joli_nom_var,
                    {self.livre.nom_table_var}.lib_long_var,
                    {self.livre.nom_table_mod}.id_mod,
                    {self.livre.nom_table_mod}.nom_mod,
                    {self.livre.nom_table_mod}.joli_nom_mod,
                    {self.livre.nom_table_mod}.lib_long_mod
                    from {self.livre.schema}.{table}
                    join {self.livre.schema}.{self.livre.nom_table_var} on {table}.id_var = {self.livre.nom_table_var}.id_var
                    join {self.livre.schema}.{self.livre.nom_table_mod} on {table}.id_mod = {self.livre.nom_table_mod}.id_mod
                    order by id_var, id_mod;
                    """
                else:
                    print("""“Ne pas choisir, c'est encore choisir.” - Jean-Paul Sartre\n Ouais ok JP mais là il faut choisir. """)
        else:
            requete_sql = f"""
            select *
            from {self.livre.schema}.{params['nom_table']}
            """
        df_table = pd.read_sql_query(requete_sql, self.livre.conn)

        choix_utilisateur = self.demander_choix_binaire("""Souhaitez-vous faire une recherche par mot_clés ?""")
        if choix_utilisateur:
            display(df_table)

            time.sleep(1)
            ok1=False
            while not ok1:
                col = input("""Ecrivez le nom de.s colonne.s sur laquelle.s vous souhaitez faire une recherche par mot-clé.\n(Séparer par une virgule et sans '' (apostrophes).)""")
                colonnes = list(col.split(','))
                if 0 not in [(col in df_table.columns)*1 for col in colonnes]:
                    ok1=True
                    ok2=False
                    while not ok2:
                        mic = input("Quel mot sera la clé ?")
                        if mic:
                            df_table[col] = df_table[col].astype(str)
                            masque = df_table[col].str.contains(str(mic), case=False, na=False)
                            df_table = df_table[masque]
                            if not df_table.empty:
                                with pd.option_context('display.max_rows', None,
                                        'display.max_columns', None,
                                        'display.precision', 3,
                                        ):
                                    display(df_table)
                            else:
                                print("""Mauvais mot-clé, pas de données...""")
                        else:
                            with pd.option_context('display.max_rows', None,
                                    'display.max_columns', None,
                                    'display.precision', 3,
                                    ):
                                display(df_table)
                        ok2 = True
                else:
                    print("""“Gouverner, c'est choisir (les bons noms de colonne).” - Duc de Lévis """)
        else:
            with pd.option_context('display.max_rows', None,
                    'display.max_columns', None,
                    'display.precision', 3,
                    ):
                display(df_table)

    def liste_table(self, afficher=True):
        
        cur = self.livre.conn.cursor()
        cur.execute(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{self.livre.schema}'
                    AND table_name !~ '^i[0-9]+'
                    AND table_type = 'BASE TABLE';
        """)
        tables = cur.fetchall()
        cur.close()
        liste = []

        if not tables:
            print('Aucune table présente dans ce schéma.')
        else:
            print(f'Tables existantes dans le schéma "{self.livre.schema}" dans PgAdmin:')
            for table in tables:
                liste.append(table[0])  
        liste.sort()
        if afficher:
            for nom_table in liste:
                print(nom_table)    
        else:
            return liste    

    @staticmethod
    def demander_choix_binaire(message):
        """
        Demande à l'utilisateur de faire un choix binaire (O/N) jusqu'à ce qu'une réponse valide soit donnée.
       
        Args:
            message (str): Le message à afficher pour demander à l'utilisateur.
           
        Returns:
            bool: True si l'utilisateur choisit 'O', False si l'utilisateur choisit 'N'.
        """
        input_ok = False
        while not input_ok:
            choix = input(message + " (O/N): ").strip().upper()
            if choix in ['O', 'N']:
                input_ok = True
                if choix == 'O':
                    print("Vous avez choisi: Oui")
                    return True
                else:
                    print("Vous avez choisi: Non")
                    return False
            else:
                print("Répondez par : O ou N")    

