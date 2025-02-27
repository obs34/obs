############
import customtkinter as ctk
from tkinter import simpledialog
from logo import Logo
from chargement_fichier import chargementFichiers
from PIL import Image


class AppVersement(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("ODH - Interface de Versement de Données")
        self.geometry("1000x1000")

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.logo = Logo("images\telechargement.png")

        self.create_widgets()

    def create_widgets(self):
        # Logo
        self.logo_frame = ctk.CTkFrame(self)
        self.logo_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)

        if self.logo.get_image():
            self.logo_label = ctk.CTkLabel(self.logo_frame, image=self.logo.get_image(), text="")
            self.logo_label.pack()
        else:
            self.logo_label = ctk.CTkLabel(self.logo_frame, text="Impossible de charger l'image")
            self.logo_label.pack()

        # Barre de menu
        self.menu_bar = ctk.CTkFrame(self)
        self.menu_bar.grid(row=1, column=0, sticky="ew")

        # Zone de sélection de l'observatoire
        self.obs_frame = ctk.CTkFrame(self)
        self.obs_frame.grid(row=2, column=0, padx=20, pady=20, sticky="ew")
        ctk.CTkLabel(self.obs_frame, text="Observatoire:", font=("Arial", 14)).pack(side="left")
        self.obs_combobox = ctk.CTkComboBox(self.obs_frame, values=["l'ODH dans bdsociohab","la MDDP", "l'ODH","l'Observatoire de test"], font=("Arial", 14))
        self.obs_combobox.pack(side="left", padx=5)

        # Zone de chargement du fichier Excel
        self.file_frame = ctk.CTkFrame(self)
        self.file_frame.grid(row=3, column=0, padx=20, pady=20, sticky="ew")
        self.file_label = ctk.CTkLabel(self.file_frame, text="Aucun fichier sélectionné", font=("Arial", 14))
        self.file_label.pack(side="left", padx=5)
        self.sheets_list = ctk.CTkTextbox(self.file_frame, height=100, font=("Arial", 12))
        self.file_loader = chargementFichiers(self.file_label, self.sheets_list)
        self.file_button = ctk.CTkButton(self.file_frame, text="Charger fichier Excel", command=self.file_loader.load_excel, font=("Arial", 14))
        self.file_button.pack(side="left")

        # Zone d'affichage des feuilles et tables
        self.sheets_frame = ctk.CTkFrame(self)
        self.sheets_frame.grid(row=4, column=0, padx=20, pady=20, sticky="nsew")
        self.sheets_list.pack(fill="both", expand=True)

        # Boutons de configuration et de versement
        self.action_frame = ctk.CTkFrame(self)
        self.action_frame.grid(row=5, column=0, padx=20, pady=20, sticky="ew")
        self.config_button = ctk.CTkButton(self.action_frame, text="Configuration", command=self.configure_versement, font=("Arial", 14))
        self.config_button.pack(side="left", padx=5)
        self.versement_button = ctk.CTkButton(self.action_frame, text="Versement", command=self.perform_versement, font=("Arial", 14))
        self.versement_button.pack(side="left", padx=5)

        # Zone d'affichage des logs
        self.log_frame = ctk.CTkFrame(self)
        self.log_frame.grid(row=6, column=0, padx=20, pady=20, sticky="nsew")
        self.log_text = ctk.CTkTextbox(self.log_frame, height=150, font=("Arial", 12))
        self.log_text.pack(fill="both", expand=True)

        # Pied de page
        self.footer_frame = ctk.CTkFrame(self)
        self.footer_frame.grid(row=7, column=0, sticky="ew", padx=10, pady=10)
        self.footer_label = ctk.CTkLabel(self.footer_frame, text="© 2025 ODH - Tous droits réservés", font=("Arial", 10))
        self.footer_label.pack()

    def configure_versement(self):
        # Ouvrir une fenêtre de configuration
        pass

    def perform_versement(self):
        # Effectuer le versement des données
        pass

    def ask_password(self):
        password = simpledialog.askstring("Mot de passe", "Entrez le mot de passe :", show="*")
        if password == "ODH2025":
            self.obs_combobox.configure(state="normal")
            self.password_button.configure(text="Accès autorisé", state="disabled")
        else:
            ctk.CTkLabel(self.obs_frame, text="Mot de passe incorrect", text_color="red", font=("Arial", 12)).pack()

