"""Module de sérialisation des données."""
from typing import Dict, List, Any
import pandas as pd
import unidecode


class SerialiseurDeDonnees:
    """Gère la sérialisation des données."""
    
    def __init__(self, livre):
        """Initialise le sérialiseur de données."""
        self.livre = livre

    def serialize_variables(self, liste_nom_feuille: list) -> pd.DataFrame:
        """
        Sérialise les variables à partir d'une liste de noms de feuilles.
        
        Args:
           liste_nom_feuille (list): Liste contenant les noms des feuilles à sérialiser.
            
        Returns:
            pd.DataFrame: Un DataFrame contenant les variables sérialisées sous forme de dictionnaires.
        """
        # Liste pour stocker les lignes sérialisées sans doublons
        lignes = []

        # Parcours de chaque nom de feuille fourni dans la liste
        for feuille in liste_nom_feuille:
            # Création d'un dictionnaire pour chaque feuille, basé sur les colonnes définies dans self.livre.colnames_var
            nouvelle_ligne = dict(zip(
                self.livre.colnames_var, # Liste des noms de colonnes attendues
                [None, 
                self._clean_string(str(feuille)),
                feuille,
                self._clean_string(str(feuille)),
                None
                ]
                ))
            # Ajout de la ligne uniquement si elle n'est pas déjà présente dans la liste
            if nouvelle_ligne not in lignes: # On évite les doublons.
                lignes.append(nouvelle_ligne)
        # Conversion de la liste de dictionnaires en un DataFrame pandas
        return pd.DataFrame.from_records(lignes)
    
    def serialize_modalites(self, liste_colonnes_sans_code: list) -> pd.DataFrame:
        """
        Sérialise les modalités uniques à partir d'une liste de colonnes.
        
        Args:
            liste_colonnes_sans_code (list): Liste des colonnes pour lesquelles extraire et sérialiser les modalités.
            
        Returns:
            pd.DataFrame: Un DataFrame contenant les modalités sérialisées sous forme de dictionnaires.
        """
        # Liste pour stocker les lignes sérialisées sans doublons
        lignes = []
        # Parcours de chaque colonne fournie dans la liste
        for col in liste_colonnes_sans_code:
            # Création d'un dictionnaire pour chaque modalité, basé sur les colonnes définies dans self.livre.colnames_mod
            nouvelle_ligne = dict(zip(self.livre.colnames_mod, # Liste des noms de colonnes attendues
                          [
                              None, 
                              self._clean_string(str(col)),# Nettoyage du nom de la colonne pour uniformisation
                              col, # Nom original de la colonne
                              self._clean_string(str(col)),# Nom nettoyé utilisé à nouveau
                              None # Valeur par défaut pour une dernière colonne (non spécifiée ici)
                          ]
                    ))
            # Ajout de la ligne uniquement si elle n'est pas déjà présente dans la liste
            if nouvelle_ligne not in lignes: # On évite les doublons.
                lignes.append(nouvelle_ligne)
        return pd.DataFrame.from_records(lignes)

    def serialize_valeurs(self, sheets_data: Dict[str, pd.DataFrame]) -> pd.DataFrame: #df_val: pd.DataFrame, 
        """
        Sérialise les valeurs issues de plusieurs feuilles de calcul.
        
        Args:
           sheets_data (Dict[str, pd.DataFrame]): Un dictionnaire où la clé est le nom de la feuille 
            et la valeur est un DataFrame contenant les données de cette feuille.
            
        Returns:
            pd.DataFrame: Un DataFrame contenant les valeurs sérialisées sous forme de dictionnaires.
        """
        #Liste pour stocker les lignes sérialisées
        lignes = []
        # Parcours des feuilles (nom de la feuille et contenu)
        for nom_feuille, df in sheets_data.items():
            # Parcours des colonnes à partir de la deuxième (on ignore la première colonne, supposée être un code d'entité)
            for col in df.columns[1:]: # On commence par la deuxième colonne puisqu'on ne veut pas la colonne code_entité.
                # Parcours des valeurs de la colonne
                for index, value in enumerate(df[col]):
                    # Création d'un dictionnaire pour chaque valeur
                    nouvelle_ligne = dict(zip(self.livre.colnames_val, 
                            [None, 
                             self.livre.id_versement, # Identifiant de versement
                             self.livre.annee, # Année associée aux données
                             self.livre.echelle, # Échelle géographique des données
                             df.loc[index, df.columns[0]], # Code de l'entité (première colonne du DataFrame)
                             self._clean_string(str(nom_feuille)),# Nom nettoyé de la feuille
                             self._clean_string(str(col)), # Nom nettoyé de la colonne
                             value])) # Valeur de la cellule
                    lignes.append(nouvelle_ligne)
        return pd.DataFrame.from_records(lignes)

    def serialize_versement(self) -> pd.DataFrame:
        """
        Sérialise les métadonnées relatives à un versement de données
        
        Args:
          pd.DataFrame: Un DataFrame contenant les métadonnées du versement sous forme sérialisée.
            
        Returns:
            Liste des valeurs sérialisées
        """
        # Création d'une ligne unique contenant les métadonnées du versement
        nouvelle_ligne = [dict(zip(self.livre.colnames_vers, #Listes des colonnes attendues pour le versement 
                            [
                            self.livre.id_versement,
                            self.livre.nom_table,
                            self.livre.annee,
                            self.livre.echelle, # La colonne 0 est code_entité.
                            self.livre.theme,
                            self.livre.source,
                            None,
                            None
                            ]))]
        # Conversion de la liste contenant le dictionnaire en un DataFrame pandas
        return pd.DataFrame.from_records(nouvelle_ligne)

    @staticmethod
    def _clean_string(text: str) -> str:
        """
        Nettoie une chaîne de caractères.
        
        Args:
            text: Texte à nettoyer
            
        Returns:
            Texte nettoyé
        """
        replacements = {
                    ' ': '_',
                    '+': '_plus_',
                    '-': '_moins_',
                    '<': '_inf_',
                    '>': '_sup_',
                    '=': '_egal_',
                    ':': '_',
                    '(': '',
                    ')': '',
                    ',': '_',
                    '\'': '_',
                    '/': '_',
                    '.': '_',
                }
        
        # Normaliser le texte (accents, caractères spéciaux)
        text = unidecode.unidecode(str(text)).lower()

        # Remplacement des caractères spécifiques
        for old, new in replacements.items():
            text = text.replace(old, new)

        # Remplacement des doubles underscores par un seul
        while '__' in text:
            text = text.replace('__', '_')

        return text.strip('_')  # Suppression des underscores en début/fin    

    def serializer(self, sheets_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Sérialise les données.
        
        Args:
            sheets_data: Données des feuilles Excel
            
        Returns:
            Données sérialisées
        """
        liste_nom_feuille = [nom_feuille for nom_feuille in list(sheets_data.keys())]
        df_var = self.serialize_variables(liste_nom_feuille)
        liste_colonnes_sans_code = [col for df in sheets_data.values() for col in df.columns[1:]]
        df_mod = self.serialize_modalites(liste_colonnes_sans_code)
        df_val = self.serialize_valeurs(sheets_data)
        df_vers = self.serialize_versement()

        return {
                self.livre.nom_table_var: df_var,
                self.livre.nom_table_mod: df_mod,
                self.livre.nom_table: df_val,
                self.livre.nom_table_vers: df_vers
            }
