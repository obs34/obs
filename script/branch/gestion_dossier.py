import datetime
import glob
import pandas as pd
import os
import shutil

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
        '''Supprime les dossiers temporaires sans demander de confirmation (celle-ci sera gérée par l'interface).'''
        dossiers = glob.glob(f'{self.livre.PREFIXE_DOSSIER_TEMPORAIRE}*')
        dossiers_supprimes = []
        if dossiers:
            for dossier in dossiers:
                try:
                    shutil.rmtree(dossier)
                    dossiers_supprimes.append(dossier)
                except OSError as e:
                    print("Error: %s - %s." % (e.filename, e.strerror))
            return dossiers_supprimes
        else:
            return []

    def processed_data_to_csv(self, processed_data: dict[str, pd.DataFrame]):
        '''Enregistre les données traitées dans un fichier CSV.'''

        for nom_table, df in processed_data.items():
            df.to_csv(f'{self.dossier_temporaire}/{nom_table}.csv', index=False, sep=self.sep, encoding=self.encoding)
            print(f"{nom_table}.csv créé.")
        
