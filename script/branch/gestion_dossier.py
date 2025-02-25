import datetime
import glob
import pandas as pd
import os
import shutil

from ..leaf.futile import demander_choix_binaire

class GestionDossier():
    def __init__(self, livre):
        self.livre = livre
        self.sep = self.livre.sep
        self.encoding = self.livre.encoding

    def create_folder(self):
        date_heure = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.dossier_temporaire = self.livre.PREFIXE_DOSSIER_TEMPORAIRE + date_heure

        os.mkdir(self.dossier_temporaire)
        print(f"Dossier traitement temporaire **{self.dossier_temporaire}** créé")
        print("Tous les fichiers csv vont être créés dans ce dossier.")

    def delete_folder(self):
        '''Supprime les dossiers temporaire.'''
        message = f'Voulez-vous supprimer tous les dossiers commençant par {self.livre.PREFIXE_DOSSIER_TEMPORAIRE} ? (O/N) '
        dossiers = glob.glob(f'{self.livre.PREFIXE_DOSSIER_TEMPORAIRE}*')
        if dossiers:
            choix = demander_choix_binaire(message)
            if choix:
                brules = []
                for dossier in dossiers:
                    try:
                        shutil.rmtree(dossier)
                        brules.append(dossier)
                    except OSError as e:
                        print("Error: %s - %s." % (e.foldername, e.strerror))
                print(f"Dossiers supprimés : {', '.join(brules)}.")
            else:
                print("Aucun dossier à supprimer.")

    def processed_data_to_csv(self, processed_data: dict[str, pd.DataFrame]):
        '''Enregistre les données traitées dans un fichier CSV.'''

        for nom_table, df in processed_data.items():
            df.to_csv(f'{self.dossier_temporaire}/{nom_table}.csv', index=False, sep=self.sep, encoding=self.encoding)
            print(f"{nom_table}.csv créé.")
        
