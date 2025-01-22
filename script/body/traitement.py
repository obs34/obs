"""Module de traitement des données."""
import psycopg2
import traceback

import script.body
from ..branch.serialiseur import SerialiseurDeDonnees
from ..branch.gestion_id import GestionId
from ..branch.gestion_dossier import GestionDossier
from ..branch.lecteur_excel_csv import LecteurExcelCsv
from ..leaf.futile import *
import script


class Traitement():
    """Gère le traitement des données."""
    
    def __init__(self, 
                 db_connection: psycopg2.extensions.connection, 
                 chemin_excel: str, 
                 livre: script.body.livre.Livre):
        """
        Initialise le processeur de données.
        
        Args:
            db_connection: Connexion à la base de données
            schema: Nom du schéma
        """
        self.db = db_connection
        self.chemin_excel = chemin_excel
        self.livre = livre
        self.lecteur = LecteurExcelCsv()
        self.serialiseur = SerialiseurDeDonnees(livre)
        self.gestion_id = GestionId(db_connection, livre)
        self.gestion_dossier = GestionDossier(livre)
    
    @combien_de_temps
    def traitement(self):
        """
        Traite les données des feuilles Excel.
        1. Lit les données
        2. "Sérialise" les données (créé 4 tables (valeur, modalité, variable, versement) à partir des feuilles Excel)
        3. Ajoute les identifiants (id_composite, id_var, id_mod, id_versement)
        4. Créé le dossier temporaire où seront stockés les CSV à verser dans la base de données
        5. Créé lesdits CSV dans le dossier temporaire
        
        Args:
            None
            Les paramètres sont dans les attributs de la classe, dans le livre.
            
        Returns:
            CSV: Fichier CSV contenant les données traitées
        """
        try:
            sheets_data = self.lecteur.read_sheets(self.chemin_excel)
            print("Données lues.")
            processed = self.serialiseur.serializer(sheets_data)
            print("Données sérialisées.")
            processed = self.gestion_id.set_id(processed)
            print("ID ajoutés.")
            self.gestion_dossier.create_file()
            print("Dossier temporaire créé.")
            self.gestion_dossier.processed_data_to_csv(processed)
            print("Données enregistrées.")
        except Exception as e:
            print(f"Erreur lors du traitement : {e}")       
            traceback.print_exc()
        
