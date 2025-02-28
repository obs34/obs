from tkinter import filedialog, messagebox
import pandas as pd
import os

class ChargementFichiers:
    def __init__(self, file_label, sheets_list):
        """
        Initialise le gestionnaire de chargement de fichier.
        
        Args:
            file_label: Le widget d'affichage du nom du fichier.
            sheets_list: Le widget d'affichage de la liste des feuilles.
        """
        self.file_label = file_label
        self.sheets_list = sheets_list

    def load_excel(self):
        """
        Ouvre une boîte de dialogue pour sélectionner un fichier Excel, 
        affiche le nom du fichier sélectionné et la liste des feuilles présentes.
        
        Returns:
            Le chemin du fichier sélectionné s'il est chargé correctement, sinon None.
        """
        # Ouvrir la boîte de dialogue pour sélectionner un fichier Excel
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
        if file_path:
            # Mettre à jour le label avec le nom du fichier sélectionné
            self.file_label.configure(text=os.path.basename(file_path))
            try:
                # Lire le fichier Excel
                excel_data = pd.ExcelFile(file_path)
                # Obtenir et afficher la liste des feuilles
                sheet_names = excel_data.sheet_names
                output = ", ".join([f" {sheet}" for sheet in sheet_names])
                self.sheets_list.delete("0.0", "end")
                self.sheets_list.insert("0.0", output)
                # Retourner le chemin du fichier pour une utilisation ultérieure
                return file_path
            except Exception as e:
                messagebox.showerror("Error", f"Erreur lors du chargement du fichier Excel :\n{e}")
                return None
        else:
            return None
