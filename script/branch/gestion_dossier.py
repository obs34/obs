import datetime
import glob
import pandas as pd
import os
import shutil

# from ..core.livre import Livre

class GestionDossier():
    def __init__(self, livre):
        self.livre = livre

    def create_folder(self):
        date_heure = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.dossier_temporaire = self.livre.PREFIXE_DOSSIER_TEMPORAIRE + date_heure

        os.mkdir(self.dossier_temporaire)
        print(f"Dossier traitement temporaire **{self.dossier_temporaire}** créé")
        print("Tous les fichiers csv vont être créés dans ce dossier.")

    def delete_folder(self):
        '''Supprime les dossiers temporaire.'''

        choix = input('Voulez-vous supprimer tous les dossiers commençant par {self.livre.PREFIXE_DOSSIER_TEMPORAIRE} ? (O/N) ').upper()
        if choix == 'O':
            # Prend tous les dossiers commençant par vars
            dossiers = glob.glob(f'{self.livre.PREFIXE_DOSSIER_TEMPORAIRE}*')
        elif choix == 'N':
            pass
        else:
            print('Choix incorrect.')

        for dossier in dossiers:
            try:
                shutil.rmtree(dossier)
                print(f"Le dossier {dossier} a été supprimé.")
            except OSError as e:
                print("Error: %s - %s." % (e.foldername, e.strerror))

    def processed_data_to_csv(self, processed_data: dict[str, pd.DataFrame]):
        '''Enregistre les données traitées dans un fichier CSV.'''

        for nom_table, df in processed_data.items():
            df.to_csv(f'{self.dossier_temporaire}/{nom_table}.csv', index=False)
            print(f"{nom_table}.csv créé.")
        
