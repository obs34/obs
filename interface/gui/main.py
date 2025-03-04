import customtkinter as ctk
import pandas as pd
from tkinter import simpledialog, messagebox
from PIL import Image
import sys
import os
import tkinter as tk

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Importation des modules internes
from interface.logo import Logo
from interface.gui.chargement_fichier import ChargementFichiers
from script.body.base_donnees import ConnectionBaseDeDonnees
from script.body.versement import Versement
from script.body.traitement import Traitement
from script.body.livre import Livre
from script.leaf.validator import DataValidator
from script.leaf.catalogue import Catalogue
from script.branch.gestion_dossier import GestionDossier
from script.branch.gomme import Gomme
from script.leaf.catalogue_interface import CatalogueInterface



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
        Cette méthode crée :
         - La zone d'affichage du logo.
         - La zone de sélection et de validation du fichier Excel.
         - La zone de sélection de l'observatoire.
         - La zone de sélection de l'échelle (pour le schéma).
         - Les champs de paramètres (année, thème, base, source).
         - Les boutons d'action (connexion, traitement, versement, nettoyage).
         - La zone des logs pour afficher les messages.
        """
        self.catalogue_btn = ctk.CTkButton(
            self, text="Ouvrir le Catalogue", command=self.open_catalogue
        )
        self.catalogue_btn.grid(row=12, column=0, padx=10, pady=5)

        # Zone du logo
        self.logo_frame = ctk.CTkFrame(self)
        self.logo_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        if self.logo.get_image():
            self.logo_label = ctk.CTkLabel(
                self.logo_frame, image=self.logo.get_image(), text=""
            )
        else:
            self.logo_label = ctk.CTkLabel(
                self.logo_frame, text="Impossible de charger l'image"
            )
        self.logo_label.pack()

        # Sélection de fichier
        self.file_label = ctk.CTkLabel(
            self, text="Aucun fichier sélectionné"
        )
        self.file_label.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        self.sheets_list = ctk.CTkTextbox(self, height=5)
        self.sheets_list.grid(row=2, column=0, padx=10, pady=5, sticky="ew")
        self.file_loader = ChargementFichiers(self.file_label, self.sheets_list)
        self.load_btn = ctk.CTkButton(
            self, text="Charger un fichier", command=self.load_and_validate_file
        )
        self.load_btn.grid(row=3, column=0, padx=10, pady=5)

        # Sélection de l'observatoire
        self.obs_frame = ctk.CTkFrame(self)
        self.obs_frame.grid(row=4, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(
            self.obs_frame,
            text="Sélectionnez un Observatoire :",
            font=("Arial", 14)
        ).pack(side="left", padx=5)
        self.observatoires = {
            "l'ODH dans bdsociohab": "1",
            "la MDDP": "2",
            "l'ODH": "3",
            "l'Observatoire de test": "4",
            "Recette ODH": "5",
            "Recette test": "6",
        }
        self.obs_combobox = ctk.CTkComboBox(
            self.obs_frame, values=list(self.observatoires.keys()), font=("Arial", 14)
        )
        self.obs_combobox.pack(side="left", padx=5)

        # Sélection de l'échelle (pour le schéma)
        self.echelle_frame = ctk.CTkFrame(self)
        self.echelle_frame.grid(row=5, column=0, padx=10, pady=5, sticky="ew")
        ctk.CTkLabel(
            self.echelle_frame,
            text="Sélectionnez l'échelle :",
            font=("Arial", 14)
        ).pack(side="left", padx=5)
        self.echelles = {
            "commune": "commune",
            "epci": "epci",
            "iris": "iris",
            "departement": "departement",
            "parcelle": "parcelle",
            "logement": "logement",
            "section_cadastrale": "section_cadastrale",
            "canton": "canton",
            "odl": "odl",
            "autre": "autre",
        }
        self.echelle_combobox = ctk.CTkComboBox(
            self.echelle_frame, values=list(self.echelles.values()), font=("Arial", 14)
        )
        self.echelle_combobox.pack(side="left", padx=5)

        # Paramètres de versement
        self.params_frame = ctk.CTkFrame(self)
        self.params_frame.grid(row=6, column=0, padx=20, pady=10, sticky="ew")
        ctk.CTkLabel(
            self.params_frame, text="Année :", font=("Arial", 14)
        ).grid(row=0, column=0, padx=5, pady=5)
        self.annee_entry = ctk.CTkEntry(self.params_frame, font=("Arial", 14))
        self.annee_entry.grid(row=0, column=1, padx=5, pady=5)

        ctk.CTkLabel(
            self.params_frame, text="Thème :", font=("Arial", 14)
        ).grid(row=1, column=0, padx=5, pady=5)
        self.theme_entry = ctk.CTkEntry(self.params_frame, font=("Arial", 14))
        self.theme_entry.grid(row=1, column=1, padx=5, pady=5)

        ctk.CTkLabel(
            self.params_frame, text="Base :", font=("Arial", 14)
        ).grid(row=2, column=0, padx=5, pady=5)
        self.base_entry = ctk.CTkEntry(self.params_frame, font=("Arial", 14))
        self.base_entry.grid(row=2, column=1, padx=5, pady=5)

        ctk.CTkLabel(
            self.params_frame, text="Source :", font=("Arial", 14)
        ).grid(row=3, column=0, padx=5, pady=5)
        self.source_entry = ctk.CTkEntry(self.params_frame, font=("Arial", 14))
        self.source_entry.grid(row=3, column=1, padx=5, pady=5)

        # Boutons d'action
        self.db_btn = ctk.CTkButton(
            self, text="Connexion à PostgreSQL", command=self.connect_to_db
        )
        self.db_btn.grid(row=7, column=0, padx=10, pady=5)
        self.process_btn = ctk.CTkButton(
            self, text="Traiter les données", command=self.process_data
        )
        self.process_btn.grid(row=8, column=0, padx=10, pady=5)
        self.versement_btn = ctk.CTkButton(
            self, text="Verser les données", command=self.perform_versement
        )
        self.versement_btn.grid(row=9, column=0, padx=10, pady=5)
        self.cleanup_btn = ctk.CTkButton(
            self, text="Nettoyer", command=self.cleanup
        )
        self.cleanup_btn.grid(row=10, column=0, padx=10, pady=5)

        # Zone des logs
        self.log_text = ctk.CTkTextbox(self, height=10)
        self.log_text.grid(row=11, column=0, padx=10, pady=10, sticky="nsew")
        self.grid_rowconfigure(11, weight=3)

    def load_and_validate_file(self):
        """
        Charge le fichier Excel et le valide en utilisant DataValidator.
        Affiche un message dans la zone des logs en cas de succès ou d'échec.
        """
        self.fichier_excel = self.file_loader.load_excel()
        if not self.fichier_excel or not DataValidator.validate_excel_file(self.fichier_excel):
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
        password = simpledialog.askstring(
            "Mot de passe",
            f"Entrez le mot de passe pour {selected_obs} :",
            show="*",
            parent=self
        )
        if not password:
            self.log_text.insert("end", "Connexion annulée.\n")
            return

        try:
            self.db = ConnectionBaseDeDonnees()
            self.conn, self.schema = self.db.connexion_observatoire(observatory_id, password)
            self.log_text.insert("end", f"Connexion réussie à {selected_obs}.\n")
        except UnicodeDecodeError:
            self.log_text.insert("end", "Erreur de connexion : Problème d'encodage détecté. Veuillez vérifier vos informations de connexion.\n")
            messagebox.showerror("Erreur","Impossible de se connecter à la base de données. Vérifier vos informations de connexion comme le mot de passe saisi.")
        except Exception:
            self.log_text.insert("end", f"Erreur de connexion : Impossible de se connecter à la base de données.\n")
            messagebox.showerror("Erreur", f"Impossible de se connecter à la base de données.\nVeuillez vérifier vos informations de connexion comme le mot de passe saisi.")

    def process_data(self):
        """
        Verification de la connexion à la base de données d'abord
        Traitement des données en créant un objet Livre et en exécutant le traitement via la classe Traitement.
        """
        if not self.conn:
            messagebox.showerror("Erreur","Veuillez d'abord vous connecter à la base de données.")
            self.log_text("end","Erreur: Connexion à la base de données non établie.\n")
            return
        try:
            annee = self.annee_entry.get()
            theme = self.theme_entry.get()
            base = self.base_entry.get()
            source = self.source_entry.get()
            echelle = self.echelle_combobox.get()

            if not all([annee, theme, base, source, echelle]):
                self.log_text.insert("end", "Veuillez remplir tous les champs de paramètres.\n")
                return

            self.livre = Livre(self.conn, theme, base, source, int(annee), self.schema)
            self.livre.echelle = echelle
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
            confirmation = messagebox.askyesno("Confirmation", "Voulez-vous supprimer tous les dossiers commençant par 'traitement_' ?")
            if confirmation:
                dossiers_supprimes = GestionDossier(self.livre).delete_folder()
                if dossiers_supprimes :
                    self.log_text.insert("end", f"Dossiers supprimés:{','.join(dossiers_supprimes)}.\n")
                else :
                    self.log_text.insert("end", "Aucun dossier à supprimer.\n")
            else:
                self.log_text.insert("end", "Nettoyage annulé.\n")
        except Exception as e:
            self.log_text.insert("end", f"Erreur lors du nettoyage : {e}\n")
            messagebox.showerror("Erreur", f"Erreur lors de la suppression des fichiers temporaires.\n{e}")

     # --- Ouverture du catalogue graphique ---
    def open_catalogue(self):
        if not self.livre:
            messagebox.showerror("Erreur", "Veuillez d'abord traiter les données.")
            return

        # Vérifier si la fenêtre Catalogue existe déjà
        if hasattr(self, "catalogue_window") and self.catalogue_window.winfo_exists():
            self.catalogue_window.lift()
            return

        # Créer une nouvelle fenêtre (Toplevel) qui prend (presque) tout l'écran
        self.catalogue_window = ctk.CTkToplevel(self)
        self.catalogue_window.title("Catalogue")

        # Définir les nouvelles dimensions pour la fenêtre
        new_width = 800  # Largeur réduite
        new_height = 600  # Hauteur réduite

        # Vous pouvez ajuster la taille en récupérant les dimensions de l'écran
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width - new_width) // 2
        y = (screen_height - new_height) // 2

        # On enlève un peu pour laisser la barre de titre accessible
        self.catalogue_window.geometry(f"{new_width}x{new_height}+{x}+{y}")
        CatalogueInterface(self.livre, self.catalogue_window)
        self.catalogue_window.lift()


if __name__ == "__main__":
    app = AppVersement()
    app.mainloop()
