import customtkinter as ctk
import pandas as pd
from tkinter import simpledialog, messagebox
from PIL import Image
import sys
import os

# Ajouter le chemin du projet aux modules importables
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Importation des modules internes
from interface.logo import Logo
from interface.gui.chargement_fichier import ChargementFichiers
from script.body.base_donnees import ConnectionBaseDeDonnees
from script.body.versement import Versement
from script.body.traitement import Traitement
from script.body.livre import Livre
from script.leaf.validator import DataValidator
from script.branch.gestion_dossier import GestionDossier
from script.branch.gomme import Gomme
#from script.leaf.futile import demander_choix_binaire

#demander_choix_binaire('Coucou !')

class AppVersement(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("ODH - Interface de Versement de Données")
        self.geometry("900x600")  # Taille adaptée à l'affichage

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure((0, 1, 2, 3, 4, 5), weight=1)

        # Chargement du logo
        self.logo = Logo()
        # Initialisation des variables pour la connexion et le versement
        self.conn = None
        self.schema = None
        self.fichier_excel = None
        self.livre = None

        self.create_widgets()

    def create_widgets(self):
        """
        Création et disposition des widgets de l'interface.
        Cette méthode crée les zones pour le logo, la sélection de fichier,
        la sélection d'observatoire, les boutons d'action et la zone des logs.
        """
        # Logo
        self.logo_frame = ctk.CTkFrame(self)
        self.logo_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        if self.logo.get_image():
            self.logo_label = ctk.CTkLabel(self.logo_frame, image=self.logo.get_image(), text="")
        else:
            self.logo_label = ctk.CTkLabel(self.logo_frame, text="Impossible de charger l'image")
        self.logo_label.pack()

        # Sélection de fichier
        self.file_label = ctk.CTkLabel(self, text="Aucun fichier sélectionné")
        self.file_label.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        self.sheets_list = ctk.CTkTextbox(self, height=5)
        self.sheets_list.grid(row=2, column=0, padx=10, pady=5, sticky="ew")
        self.file_loader = ChargementFichiers(self.file_label, self.sheets_list)
        self.load_btn = ctk.CTkButton(self, text="Charger un fichier", command=self.load_and_validate_file)
        self.load_btn.grid(row=3, column=0, padx=10, pady=5)

        # Sélection de l'observatoire
        self.obs_frame = ctk.CTkFrame(self)
        self.obs_frame.grid(row=4, column=0, padx=20, pady=10, sticky="ew")
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

        # Boutons d'action
        self.db_btn = ctk.CTkButton(self, text="Connexion à PostgreSQL", command=self.connect_to_db)
        self.db_btn.grid(row=5, column=0, padx=10, pady=5)
        self.process_btn = ctk.CTkButton(self, text="Traiter les données", command=self.process_data)
        self.process_btn.grid(row=6, column=0, padx=10, pady=5)
        self.versement_btn = ctk.CTkButton(self, text="Verser les données", command=self.perform_versement)
        self.versement_btn.grid(row=7, column=0, padx=10, pady=5)
        self.cleanup_btn = ctk.CTkButton(self, text="Nettoyer", command=self.cleanup)
        self.cleanup_btn.grid(row=8, column=0, padx=10, pady=5)

        # Zone des logs
        self.log_text = ctk.CTkTextbox(self, height=10)
        self.log_text.grid(row=9, column=0, padx=10, pady=10, sticky="nsew")
        self.grid_rowconfigure(9, weight=3)

    def load_and_validate_file(self):
        """
        Charge le fichier Excel et le valide en utilisant DataValidator.
        Affiche un message dans la zone des logs en cas de succès ou d'échec.
        """
        self.fichier_excel = self.file_loader.load_excel()
        if not self.fichier_excel: #or not DataValidator.validate_excel_file(self.fichier_excel):
            self.log_text.insert("end", "Fichier invalide !\n")
            return
        self.log_text.insert("end", "Fichier validé !\n")

    def connect_to_db(self):
        """
        Connexion à la base de données PostgreSQL via l'interface.
        Demande à l'utilisateur de sélectionner un observatoire et d'entrer le mot de passe.
        """
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
            messagebox.showerror("Erreur", f"Impossible de se connecter à la base de données.\n{e}")

    def process_data(self):
        """
        Traite les données en créant un objet Livre et en exécutant le traitement via la classe Traitement.
        """
        try:
            self.livre = Livre(self.conn, "theme", "base", "source", 2025, self.schema)
            traiteur = Traitement(self.conn, self.fichier_excel, self.livre)
            traiteur.traitement()
            self.log_text.insert("end", "Données traitées !\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur lors du traitement : {e}\n")
            messagebox.showerror("Erreur", f"Erreur lors du traitement des données.\n{e}")

    def perform_versement(self):
        """
        Verse les données traitées dans la base de données en utilisant la classe Versement.
        """
        try:
            versement = Versement(self.conn, self.livre)
            versement.versement()
            self.log_text.insert("end", "Versement terminé !\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur de versement : {e}\n")
            messagebox.showerror("Erreur", f"Erreur lors du versement des données.\n{e}")

    def cleanup(self):
        """
        Supprime les fichiers temporaires créés durant le processus de traitement.
        """
        try:
            GestionDossier(self.livre).delete_folder()
            self.log_text.insert("end", "Nettoyage terminé !\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur lors du nettoyage : {e}\n")
            messagebox.showerror("Erreur", f"Erreur lors de la suppression des fichiers temporaires.\n{e}")

if __name__ == "__main__":
    app = AppVersement()
    app.mainloop()
