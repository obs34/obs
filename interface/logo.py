import os
from PIL import Image
import customtkinter as ctk

class Logo:

    def __init__(self, filename="téléchargement.png", size=(600, 300)):
        # Obtenir le chemin absolu de l'image dans le dossier "images"
        logo_path = os.path.join(os.path.dirname(__file__), "images", filename)

        try:
            image = Image.open(logo_path)
            self.logo_image = ctk.CTkImage(light_image=image, size=size)
        except Exception as e:
            print(f"Erreur lors du chargement de l'image : {e}")
            self.logo_image = None

    def get_image(self):
        """Retourne l'image si elle est chargée, sinon None"""
        return self.logo_image
