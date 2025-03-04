import customtkinter as ctk
import pandas as pd
from tkinter import messagebox
from script.leaf.catalogue import Catalogue  # On peut réutiliser certaines fonctions back-end

class CatalogueInterface:
    def __init__(self, livre, parent_window):
        self.livre = livre
        self.parent = parent_window
        # On peut instancier le catalogue back-end si nécessaire
        self.catalogue_backend = Catalogue(livre)
        self.setup_interface()

    def setup_interface(self):
        # Titre et instruction
        ctk.CTkLabel(self.parent, text="Liste des dictionnaires disponibles :", font=("Arial", 14)).pack(pady=10)

        # Dictionnaires à choisir
        self.dictionnaires = {
            "1": "Recherche sur les modalités et variables d'une table",
            "2": "Recherche sur la table des modalités",
            "3": "Recherche sur la table des variables",
            "4": "Recherche sur le dictionnaire des tables"
        }
        for key, text in self.dictionnaires.items():
            ctk.CTkLabel(self.parent, text=f"{key}: {text}").pack()

        # Combobox pour le choix
        self.dictionnaire_var = ctk.StringVar()
        ctk.CTkComboBox(self.parent, values=list(self.dictionnaires.keys()), variable=self.dictionnaire_var, font=("Arial", 14)).pack(pady=10)

        # Bouton pour valider le choix
        ctk.CTkButton(self.parent, text="Valider", command=self.handle_catalogue_choice).pack(pady=10)

    def handle_catalogue_choice(self):
        choix = self.dictionnaire_var.get()
        if choix == "1":
            # Obtenir la liste des tables via la méthode back-end
            liste_table = self.catalogue_backend.liste_table(afficher=False)
            self.show_table_selection(liste_table)
        elif choix in ["2", "3", "4"]:
            params = {
                "2": self.livre.nom_table_mod,
                "3": self.livre.nom_table_var,
                "4": self.livre.nom_table_vers,
            }
            requete_sql = f"SELECT * FROM {self.livre.schema}.{params[choix]}"
            try:
                df = pd.read_sql_query(requete_sql, self.livre.conn)
                self.display_dataframe(df)
            except Exception as e:
                messagebox.showerror("Erreur", f"Erreur lors de la requête : {e}")
        else:
            messagebox.showerror("Erreur", "Choix invalide. Veuillez sélectionner une option valide.")

    def show_table_selection(self, liste_table):
        # Fenêtre pour la sélection de table
        self.table_selection_window = ctk.CTkToplevel(self.parent)
        self.table_selection_window.title("Sélection de table")
        ctk.CTkLabel(self.table_selection_window, text="Liste des tables disponibles :", font=("Arial", 14)).pack(pady=10)
        self.table_var = ctk.StringVar()
        ctk.CTkComboBox(self.table_selection_window, values=liste_table, variable=self.table_var, font=("Arial", 14)).pack(pady=10)
        ctk.CTkButton(self.table_selection_window, text="Valider", command=self.handle_table_selection).pack(pady=10)

    def handle_table_selection(self):
        table = self.table_var.get()
        if not table:
            messagebox.showerror("Erreur", "Veuillez sélectionner une table.")
            return
        requete_sql = f"""
        SELECT DISTINCT
            {self.livre.nom_table_var}.id_var,
            {self.livre.nom_table_var}.nom_var,
            {self.livre.nom_table_var}.joli_nom_var,
            {self.livre.nom_table_var}.lib_long_var,
            {self.livre.nom_table_mod}.id_mod,
            {self.livre.nom_table_mod}.nom_mod,
            {self.livre.nom_table_mod}.joli_nom_mod,
            {self.livre.nom_table_mod}.lib_long_mod
        FROM {self.livre.schema}.{table}
        JOIN {self.livre.schema}.{self.livre.nom_table_var} ON {table}.id_var = {self.livre.nom_table_var}.id_var
        JOIN {self.livre.schema}.{self.livre.nom_table_mod} ON {table}.id_mod = {self.livre.nom_table_mod}.id_mod
        ORDER BY id_var, id_mod;
        """
        try:
            df = pd.read_sql_query(requete_sql, self.livre.conn)
            self.display_dataframe(df)
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur lors de la requête : {e}")

    def display_dataframe(self, df):
        result_window = ctk.CTkToplevel(self.parent)
        result_window.title("Résultats de la Recherche")
        result_window.geometry("1000x600")
        text_box = ctk.CTkTextbox(result_window, wrap="none")
        text_box.pack(expand=True, fill="both")
        text_box.insert("end", df.to_string(index=False))
