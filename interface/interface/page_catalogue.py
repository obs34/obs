import pandas as pd
import time
import customtkinter as ctk
from tkinter import simpledialog, messagebox, ttk

from interface.base_donnees.base_donnees import ConnectionBaseDeDonnees
from script.body.livre import Livre
from script.leaf.catalogue import Catalogue

class AppCatalogue(ctk.CTkFrame):
    def __init__(self, master, app):
        super().__init__(master)

        self.app = app  # sauvegarde la référence vers l'application principale

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(4, weight=1)

        self.create_widgets()
        
    def create_widgets(self):
        # Le bouton doit être dans la méthode __init__
        self.btn_accueil = ctk.CTkButton(
            self, 
            text="Accueil", 
            command=lambda: self.app.show_page("AppVersement")
        )
        self.btn_accueil.grid(row=1, column=0, padx=10, pady=10)

        # Sélection de l'observatoire
        self.obs_frame = ctk.CTkFrame(self)
        self.obs_frame.grid(row=2, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(self.obs_frame, text="Sélectionnez un Observatoire :", font=("Arial", 14)).pack(side="left", padx=5)
        self.observatoires = {
            "l'ODH dans bdsociohab": "1",
            "la MDDP": "2",
            "l'ODH": "3",
            "l'Observatoire de test": "4",
            "Recette ODH": "5",
            "Recette test": "6",
        }
        self.obs_combobox = ctk.CTkComboBox(self.obs_frame, values=list(self.observatoires.keys()), font=("Arial", 14))
        self.obs_combobox.pack(side="left", padx=5)

        # Bouton connexion DB
        self.db_btn = ctk.CTkButton(self, text="Connexion à PostgreSQL", command=self.connect_to_db)
        self.db_btn.grid(row=3, column=0, padx=12, pady=5)

        # Liste des tables
        self.load_btn = ctk.CTkButton(self, text="Liste des tables", command=self.fun_liste_table)
        self.load_btn.grid(row=4, column=0, padx=10, pady=5)

        # Sélection du type de catalogue
        self.obs_frame = ctk.CTkFrame(self)
        self.obs_frame.grid(row=5, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(self.obs_frame, text="Sélectionnez un catalogue :", font=("Arial", 14)).pack(side="left", padx=5)
        self.type_catalogue = {
            "Modalités et variables d'une table.": None,
            "Table des modalités." : "",# self.livre.nom_table_mod,
            "Table des variables." : "",# self.livre.nom_table_var,
            "Dictionnaire des versements." : "",# self.livre.nom_table_vers,
        }
        self.catalogue_combobox = ctk.CTkComboBox(self.obs_frame, values=list(self.type_catalogue.keys()), font=("Arial", 14))
        self.catalogue_combobox.pack(side="left", padx=5)

        # Champs de saisie : table
        self.nom_table = ctk.CTkEntry(self, placeholder_text="Nom d'une table")
        self.nom_table.grid(row=6, column=0, padx=5, pady=5)

        # # Champs de saisie : mot-clé
        # self.motcle = ctk.CTkEntry(self, placeholder_text="mot-clé")
        # self.motcle.grid(row=7, column=0, padx=5, pady=5)

        # Catalogue
        self.load_btn = ctk.CTkButton(self, text="Catalogue", command=self.fun_catalogue)
        self.load_btn.grid(row=7, column=0, padx=10, pady=5)

        # Zone des logs
        self.log_text = ctk.CTkTextbox(self, height=20)
        self.log_text.grid(row=8, column=0, padx=10, pady=10, sticky="nsew")
        self.grid_rowconfigure(8, weight=3)  # permet une expansion dynamique

    def connect_to_db(self):
        selected_obs = self.obs_combobox.get()
        if not selected_obs:
            self.log_text.insert("end", "Veuillez sélectionner un observatoire.\n")
            return
        observatory_id = self.observatoires[selected_obs]
        password = simpledialog.askstring("Mot de passe", f"Entrez le mot de passe pour {selected_obs} :", show="*")
        if not password:
            self.log_text.insert("end", "Connexion annulée.\n")
            return
        try:
            self.db = ConnectionBaseDeDonnees()
            self.conn, self.schema = self.db.connexion_observatoire(observatory_id, password)
            self.log_text.insert("end", f"Connexion réussie à {selected_obs}.\n")
            self.livre = Livre(self.conn, schema=self.schema, theme="", base="", source="", annee="")
            self.catalogue = Catalogue(self.livre)

            # Mise à jour complète de type_catalogue
            self.type_catalogue = {
                "Modalités et variables d'une table.": None,
                "Table des modalités.": self.livre.nom_table_mod,
                "Table des variables.": self.livre.nom_table_var,
                "Dictionnaire des versements.": self.livre.nom_table_vers,
            }

            # Mettre à jour les valeurs du combobox après connexion
            self.catalogue_combobox.configure(values=list(self.type_catalogue.keys()))
            
        except Exception as e:
            self.log_text.insert("end", f"Erreur de connexion : {e}\n")
            messagebox.showerror("Erreur", f"Impossible de se connecter à la base.\n{e}")

    def fun_liste_table(self):
        if not hasattr(self, 'conn') or not self.conn:
            self.log_text.insert("end", "Veuillez d'abord vous connecter à la base de données.\n")
            return

        try:
            tables = self.catalogue.liste_table(afficher=False)  # Supposons que la méthode s'appelle comme ceci
            
            self.log_text.insert("end", "Tables disponibles :\n")
            for table in tables:
                self.log_text.insert("end", f"- {table}\n")

        except Exception as e:
            self.log_text.insert("end", f"Erreur lors du listage des tables : {e}\n")
            messagebox.showerror("Erreur", f"Erreur : {e}")

    def display_catalogue(self, selected_catalogue):

        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

        # https://www.geeksforgeeks.org/how-to-print-an-entire-pandas-dataframe-in-python/

        if selected_catalogue=="Modalités et variables d'une table.":
            table = self.nom_table.get()
            if table in self.catalogue.liste_table(afficher=False):
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
            from {self.livre.schema}.{self.type_catalogue[selected_catalogue]}
            """
        df_table = pd.read_sql_query(requete_sql, self.livre.conn)

        # Affiche ton DataFrame avec le Treeview
        self.afficher_dataframe_treeview(df_table)
 

    def fun_catalogue(self):
        """
        Affiche le catalogue de la base de données.
        """
        selected_catalogue = self.catalogue_combobox.get()
        # motcle = self.motcle.get()
        self.display_catalogue(selected_catalogue)

    def afficher_dataframe_treeview(self, df):
        # Fenêtre popup avec le tableau
        popup = ctk.CTkToplevel(self)
        popup.title("Affichage du catalogue")
        popup.geometry("1200x600")

        # Frame conteneur
        frame = ctk.CTkFrame(popup)
        frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Scrollbars
        scrollbar_y = ctk.CTkScrollbar(frame, orientation="vertical")
        scrollbar_y.pack(side="right", fill="y")

        scrollbar_x = ctk.CTkScrollbar(frame, orientation="horizontal")
        scrollbar_x.pack(side="bottom", fill="x")

        # Création du Treeview
        tree = ttk.Treeview(frame, columns=list(df.columns), show="headings",
                            yscrollcommand=scrollbar_y.set,
                            xscrollcommand=scrollbar_x.set)
        tree.pack(expand=True, fill="both")

        scrollbar_y.configure(command=tree.yview)
        scrollbar_x.configure(command=tree.xview)

        # Colonnes
        for col in df.columns:
            tree.heading(col, text=col)
            tree.column(col, width=200, anchor="center")

        # Remplissage des lignes
        for _, row in df.iterrows():
            tree.insert("", "end", values=list(row))
