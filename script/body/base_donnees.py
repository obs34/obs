"""Module de gestion de la connexion à la base de données."""
import getpass
import psycopg2
from psycopg2.extensions import connection
from typing import Optional,Tuple
from IPython.display import clear_output

class ConnectionBaseDeDonnees:
    """Gère la connexion à la base de données PostgreSQL."""
    
    def __init__(self):
        """Initialise la connexion à la base de données."""
        # load_dotenv()
        self.connection: Optional[connection] = None
        self.schema: Optional[str] = None
        
    def connexion_observatoire(self, observatory_id : str, password : str) -> Tuple[connection, str]:
        """
        Établit une connexion à l'observatoire choisi.
        Args:
            observatory_id (str): L'identifiant de l'observatoire choisi.
            password (str): Le mot de passe pour la connexion.
        
        Returns:
            tuple: (connexion psycopg2, nom du schéma)
        Raises:
            ConnectionError: En cas d'échec de connexion à la base.
            ValueError: Si l'observatoire est invalide.
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
            },
            '5': {
                'name': "Recette ODH",
                'params': {
                    'database': "obs",
                    'user': "hab",
                    'host': "s1943",
                    'port': "5432",
                    'schema': "hab"
                }
            },
            '6': {
                'name': "Recette test",
                'params': {
                    'database': "obs",
                    'user': "test",
                    'host': "s1943",
                    'port': "5432",
                    'schema': "test"
                }
            },
        }

        print("Liste des observatoires disponibles :")
        for key, obs in observatories.items():
            print(f"{key} : {obs['name']}")
        
        
        if observatory_id not in observatories:
            raise ValueError("Observatoire invalide. Sélectionnez une option valide.")

        obs = observatories[observatory_id]

        try:
            self.connection = psycopg2.connect(
                database=obs['params']['database'],
                user=obs['params']['user'],
                password=password,  # Utilisation du mot de passe fourni
                host=obs['params']['host'],
                port=obs['params']['port']
            )
            self.schema = obs['params']['schema']
            print(f"Connexion réussie à {obs['name']} !")
            return self.connection, self.schema
        except psycopg2.Error as e:
            raise ConnectionError(f"Erreur de connexion : {e}")

    def close(self):
        """Ferme la connexion à la base de données."""
        if self.connection:
            self.connection.close()
            print("Connexion fermée.")