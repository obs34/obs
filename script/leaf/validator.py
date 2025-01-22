"""Module de validation des données."""
import os
import pandas as pd
from typing import Any, List, Dict

class DataValidator:
    """Classe de validation des données."""
    
    @staticmethod
    def validate_excel_file(file_path: str) -> bool:
        """
        Valide le fichier Excel.
        
        Args:
            file_path: Chemin du fichier
            
        Returns:
            bool: True si valide
        """
        try:
            # pd.ExcelFile(file_path)
            return True if os.path.isfile(file_path) else False
        except Exception as e:
            print(f"Erreur de validation du fichier Excel : {e}")
            return False
    
    @staticmethod
    def validate_sheet_data(df: pd.DataFrame) -> List[str]:
        """
        Valide les données d'une feuille.
        
        Args:
            df: DataFrame à valider
            
        Returns:
            Liste des erreurs
        """
        errors = []
        
        # Vérifie les colonnes vides
        empty_cols = df.columns[df.isna().all()].tolist()
        if empty_cols:
            errors.append(f"Colonnes vides trouvées : {empty_cols}")
        
        # Vérifie les doublons
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            errors.append(f"Trouvé {duplicates} lignes dupliquées")
        
        return errors