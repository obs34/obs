"""Module de gestion de la connexion à la base de données."""
import getpass
import psycopg2
from psycopg2.extensions import connection
from typing import Optional
from IPython.display import clear_output

class ConnectionBaseDeDonnees:
    """Gère la connexion à la base de données PostgreSQL."""
    
    def __init__(self):
        """Initialise la connexion à la base de données."""
        # load_dotenv()
        self.connection: Optional[connection] = None
        self.schema: Optional[str] = None
        
    def connexion_observatoire(self) -> tuple[connection, str]:
        """
        Établit une connexion à l'observatoire choisi.
        
        Returns:
            tuple: (connexion psycopg2, nom du schéma)
        """
        observatories = {
            '1': {
                'name': "l'ODH dans bdsociohab",
                'params': {
                    'database': "bdsociohab",
                    'user': "admoddh",
                    'host': "s934",
                    'port': "5441",
                    'schema': "socio_hab_test"
                }
            },
            '2': {
                'name': "la MDDP",
                'params': {
                    'database': "bdsociohab",
                    'user': "admmddep",
                    'host': "s934",
                    'port': "5441",
                    'schema': "test"
                }
            },
            '3': {
                'name': "l'ODH",
                'params': {
                    'database': "obs",
                    'user': "hab",
                    'host': "s934",
                    'port': "5441",
                    'schema': "hab"
                }
            },
            '4': {
                'name': "l'Observatoire de test",
                'params': {
                    'database': "obs",
                    'user': "test_obs",
                    'host': "s934",
                    'port': "5441",
                    'schema': "test_obs"
                }
            }
        }

        print("Liste des observatoires disponibles :", flush=True)
        for key, obs in observatories.items():
            print(f"{key} : {obs['name']}", flush=True)
            
        while True:
            choice = input("Choisissez un observatoire (numéro) : ")
            if choice in observatories:
                obs = observatories[choice]
                mdp = getpass.getpass(f"Entrez votre mot de passe pour le schéma {obs['params']['schema']} de la base de donnée {obs['params']['database']} : ")
                try:
                    # password = os.getenv(f"DB_PASSWORD_{choice}", "")
                    self.connection = psycopg2.connect(
                        database=obs['params']['database'],
                        user=obs['params']['user'],
                        password=mdp,
                        host=obs['params']['host'],
                        port=obs['params']['port']
                    )
                    self.schema = obs['params']['schema']
                    clear_output(wait=True)
                    print(f"Connexion réussie à {obs['name']} !")
                    return self.connection, self.schema
                except psycopg2.Error as e:
                    print(f"Erreur de connexion : {e}")
            else:
                print("Choix invalide")
    
    def close(self):
        """Ferme la connexion à la base de données."""
        if self.connection:
            self.connection.close()