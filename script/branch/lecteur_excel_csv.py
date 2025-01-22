"""Module de lecture des fichiers Excel."""
from typing import Dict
import pandas as pd

class LecteurExcelCsv:
    """Gère la lecture des fichiers Excel."""
    
    def __init__(self):
        """
        Initialise le lecteur Excel.
        
        Args:
            file_path: Chemin vers le fichier Excel
        """
        pass
    
    def read_sheets(self, file_path) -> Dict[str, pd.DataFrame]:
        """
        Lit toutes les feuilles du fichier Excel.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des feuilles Excel
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            sheets = {}
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                # Nettoie les noms de colonnes
                df.columns = self._clean_column_names(df.columns)
                sheets[sheet_name] = df
            return sheets
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier Excel : {e}")
            raise
    
    @staticmethod # https://docs.python.org/3/library/functions.html#staticmethod
    def _clean_column_names(columns):
        """
        Nettoie les noms des colonnes.
        
        Args:
            columns: Liste des noms de colonnes
            
        Returns:
            Liste des noms de colonnes nettoyés
        """
        replacements = {
            ' ': '',
            '+': '_plus_',
            '-': '_moins_',
            '<': '_inf_',
            '>': '_sup_',
            '=': '_egal_',
            ':': '_',
            '(': '',
            ')': ''
        }
        
        clean_columns = columns.copy()
        for old, new in replacements.items():
            clean_columns = clean_columns.str.replace(old, new, regex=False)
        
        return clean_columns
    
    def read_csv(self, file_path, sep: str = ',') -> pd.DataFrame:
        """
        Lit un fichier CSV.
        
        Args:
            sep: Séparateur de colonnes
        
        Returns:
            pd.DataFrame: Données du fichier CSV
        """
        try:
            return pd.read_csv(file_path, sep=sep, encoding="utf-8-sig")
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier CSV : {e}")
            raise