import customtkinter as ctk
import pandas as pd
from tkinter import simpledialog, messagebox, ttk
import tksheet
import sys
import os
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from interface.interface.chargement_fichier import chargementFichiers
# from interface.base_donnees.base_donnees import ConnectionBaseDeDonnees
from script.body.base_donnees import ConnectionBaseDeDonnees
from script.body.versement import Versement
from script.body.traitement import Traitement
from script.body.livre import Livre
from script.branch.gestion_dossier import GestionDossier

class AppVersement(ctk.CTkFrame):
    def __init__(self, master, app):
        super().__init__(master)

        self.app = app  # référence à l'application principale pour la navigation

        self.conn = None
        self.schema = None
        self.fichier_excel = None
        self.livre = None

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(14, weight=3)

        self.create_widgets()

    def create_widgets(self):
        # Bouton vers Catalogue
        self.btn_catalogue = ctk.CTkButton(
            self, text="Catalogue",
            command=lambda: self.app.show_page("AppCatalogue")  # Navigation corrigée
        )
        self.btn_catalogue.grid(row=1, column=0, padx=10, pady=10)

        # Sélection de fichier
        self.file_label = ctk.CTkLabel(self, text="Aucun fichier sélectionné")
        self.file_label.grid(row=2, column=0, padx=10, pady=5, sticky="ew")
        self.sheets_list = ctk.CTkTextbox(self, height=5)
        self.sheets_list.grid(row=3, column=0, padx=10, pady=5, sticky="ew")
        self.file_loader = chargementFichiers(self.file_label, self.sheets_list)
        self.load_btn = ctk.CTkButton(self, text="Charger un fichier", command=self.load_and_validate_file)
        self.load_btn.grid(row=4, column=0, padx=10, pady=5)

        # Sélection de l'observatoire
        self.obs_frame = ctk.CTkFrame(self)
        self.obs_frame.grid(row=5, column=0, padx=20, pady=10, sticky="ew")
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

        # Connexion DB
        self.db_btn = ctk.CTkButton(self, text="Connexion à PostgreSQL", command=self.connect_to_db)
        self.db_btn.grid(row=6, column=0, padx=12, pady=5)

        # Champs de saisie
        self.entree1 = ctk.CTkEntry(self, placeholder_text="thème")
        self.entree1.grid(row=7, column=0, padx=5, pady=5)

        self.entree2 = ctk.CTkEntry(self, placeholder_text="base")
        self.entree2.grid(row=8, column=0, padx=5, pady=5)

        self.entree3 = ctk.CTkEntry(self, placeholder_text="source")
        self.entree3.grid(row=9, column=0, padx=5, pady=5)

        self.entree4 = ctk.CTkEntry(self, placeholder_text="année")
        self.entree4.grid(row=10, column=0, padx=5, pady=5)

        # Actions
        self.process_btn = ctk.CTkButton(self, text="Traiter les données", command=self.process_data)
        self.process_btn.grid(row=11, column=0, padx=12, pady=5)

        self.versement_btn = ctk.CTkButton(self, text="Verser les données", command=self.perform_versement)
        self.versement_btn.grid(row=12, column=0, padx=12, pady=5)

        self.cleanup_btn = ctk.CTkButton(self, text="Nettoyer", command=self.cleanup)
        self.cleanup_btn.grid(row=13, column=0, padx=12, pady=5)

        # Zone des logs
        self.log_text = ctk.CTkTextbox(self, height=10)
        self.log_text.grid(row=14, column=0, padx=10, pady=10, sticky="nsew")

        # Liste des fichiers CSV disponibles
        self.csv_files_combobox = ctk.CTkComboBox(self, values=[], font=("Arial", 14))
        self.csv_files_combobox.grid(row=15, column=0, padx=10, pady=5, sticky="ew")

        # Bouton pour charger les fichiers CSV
        self.load_csv_btn = ctk.CTkButton(self, text="Charger les CSV", command=self.load_csv_files)
        self.load_csv_btn.grid(row=16, column=0, padx=10, pady=5)

        # Bouton pour ouvrir et éditer le CSV sélectionné
        self.edit_csv_btn = ctk.CTkButton(self, text="Éditer CSV sélectionné", command=self.edit_selected_csv)
        self.edit_csv_btn.grid(row=17, column=0, padx=10, pady=5)

    # Méthodes existantes inchangées
    def load_and_validate_file(self):
        self.fichier_excel = self.file_loader.load_excel()
        if not self.fichier_excel:
            self.log_text.insert("end", "Fichier invalide !\n")
            return
        self.log_text.insert("end", "Fichier validé !\n")

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
        except Exception as e:
            self.log_text.insert("end", f"Erreur de connexion : {e}\n")
            messagebox.showerror("Erreur", f"Impossible de se connecter à la base.\n{e}")

    def process_data(self):
        try:
            self.livre = Livre(self.conn, schema=self.schema, theme=self.entree1.get(),
                               base=self.entree2.get(), source=self.entree3.get(), annee=self.entree4.get())
            Traitement(self.conn, self.fichier_excel, self.livre).traitement()
            self.log_text.insert("end", "Données traitées !\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur lors du traitement : {e}\n")
            messagebox.showerror("Erreur", f"Erreur :\n{e}")

    def perform_versement(self):
        try:
            Versement(self.conn, self.livre).versement()
            self.log_text.insert("end", "Versement terminé !\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur de versement : {e}\n")
            messagebox.showerror("Erreur", f"Erreur :\n{e}")

    def cleanup(self):
        try:
            GestionDossier(self.livre).delete_folder()
            self.log_text.insert("end", "Nettoyage terminé !\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur : {e}\n")
            messagebox.showerror("Erreur", f"Erreur :\n{e}")

    def load_csv_files(self):
        # Chemin relatif vers le dossier contenant les CSV (à partir du fichier Python exécuté)
        dossier_data = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
        dossier_csv = sorted(glob.glob(f'traitement_*'), key=os.path.getmtime)[-1] # {self.livre.PREFIXE_DOSSIER_TEMPORAIRE}
        chemin_csv = os.path.join(dossier_data, dossier_csv)
        fichiers_csv = glob.glob(os.path.join(chemin_csv, "*.csv"))
        if fichiers_csv:
            self.csv_files_combobox.configure(values=fichiers_csv)
            self.log_text.insert("end", f"Fichiers CSV chargés : {len(fichiers_csv)} trouvés.\n")
        else:
            self.log_text.insert("end", "Aucun fichier CSV trouvé.\n")

    def edit_selected_csv(self):
        fichier_csv = self.csv_files_combobox.get()

        if not fichier_csv:
            messagebox.showwarning("Attention", "Aucun fichier CSV sélectionné !")
            return

        df = pd.read_csv(fichier_csv)

        popup = ctk.CTkToplevel(self)
        popup.title(f"Édition du CSV : {os.path.basename(fichier_csv)}")
        popup.geometry("1000x600")

        sheet = tksheet.Sheet(popup, data=df.values.tolist(), headers=df.columns.tolist())
        sheet.enable_bindings()
        sheet.pack(fill="both", expand=True, padx=10, pady=10)

        def sauvegarder_csv():
            new_data = sheet.get_sheet_data()  # sans argument
            df_modifie = pd.DataFrame(new_data, columns=df.columns)
            df_modifie.to_csv(fichier_csv, index=False)
            messagebox.showinfo("Succès", "Modifications sauvegardées avec succès.")

        btn_save = ctk.CTkButton(popup, text="Sauvegarder", command=sauvegarder_csv)
        btn_save.pack(pady=10)

