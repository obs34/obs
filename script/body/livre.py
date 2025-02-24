from IPython.display import clear_output
from typing import Optional

class Livre:
    '''Classe contenant les informations sur le livre de données.'''
    def __init__(self, conn, theme: str, base: str, source: str, annee: int, schema: str):
        
        self.conn = conn
        self.theme = theme
        self.base = base
        self.source = source
        self.annee = annee
        self.schema = schema

        self.PREFIXE_DOSSIER_TEMPORAIRE = 'traitement_'
        self.colnames_val = ['id_composite', 'id_versement', 'annee', 'echelle', 'code_entite', 'id_var', 'id_mod', 'valeur']
        self.colnames_var = ['id_var', 'nom_var', 'joli_nom_var', 'var_regroupement', 'lib_long_var']
        self.colnames_mod = ['id_mod', 'nom_mod', 'joli_nom_mod', 'mod_regroupement', 'lib_long_mod']
        self.colnames_vers = ['id_versement', 'nom_table', 'annee', 'echelle', 'theme', 'source', 'commentaire', 'url']

        PREFIXE = 'texte'
        self.nom_table_var = f'{PREFIXE}_{self.schema}_var'.lower()
        self.nom_table_mod = f'{PREFIXE}_{self.schema}_mod'.lower()
        self.nom_table_vers = f'{PREFIXE}_{self.schema}_vers'.lower()
        self.nom_table = f"{theme}_{base}_{source}"

        self.echelle: Optional[str] = None
        self.id_versement: Optional[int] = None

        self.relations = {
            'primaire': {
                self.nom_table : self.colnames_val[0],
                self.nom_table_mod : self.colnames_mod[0],
                self.nom_table_var : self.colnames_var[0],
                self.nom_table_vers : self.colnames_vers[0]
            },
            'etrangere': {
                self.nom_table_mod : (self.colnames_mod[0], self.colnames_val[6]),
                self.nom_table_var : (self.colnames_var[0], self.colnames_val[5]),
                self.nom_table_vers : (self.colnames_vers[0], self.colnames_val[1])
            }
        }

    @staticmethod
    def choix_echelle() -> str:
        """
        Choisir l'échelle des données.
        """
        echelles = {
            1: 'commune',
            2: 'epci',
            3: 'iris',
            4: 'departement',
            5: 'parcelle',
            6: 'logement',
            7: 'section_cadastrale',
            8: 'canton',
            9: 'odl',
            10: 'autre'
        }

        echelle_ok = False
        print("Liste des observatoires disponibles :", flush=True)
        for numero, echelle in echelles.items():
            print(f"{numero} : {echelle}", flush=True)
        clear_output(wait=True)
            
        while not echelle_ok:
            echelle_numero = input("Veuillez choisir l'échelle correspondant à l'échelle de vos données :")
            try:
                echelle_int = int(echelle_numero)
                if echelle_int in echelles and echelle_int != 9:
                    echelle = echelles[echelle_int]
                    print(f"Vous avez choisi l'échelle {echelle}.")
                    echelle_ok = True
                elif echelle_int == 9:
                    echelle = input("Veuillez saisir l'échelle correspondant à l'échelle de vos données :")
                    print(f"Vous avez choisi l'échelle {echelle}.")
                    echelle_ok = True
                else:
                    print('Réponse invalide')
            except ValueError:
                print('Réponse invalide')
        return echelle

