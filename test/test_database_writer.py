# test_database_writer.py
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from database_writer import DatabaseWriter

class TestDatabaseWriter(unittest.TestCase):
    @patch('database_writer.psycopg2.connect')
    def test_write(self, mock_connect):
        # Créer un DataFrame de test
        data = {'Col1': [1, 2, 3], 'Col2': ['A', 'B', 'C']}
        df = pd.DataFrame(data)

        # Configurer le mock pour la connexion à la base de données
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Écrire les données avec DatabaseWriter
        writer = DatabaseWriter('fake_connection_string')
        writer.write(df)

        # Vérifier que les appels à la base de données sont corrects
        mock_cursor.execute.assert_called_with("INSERT INTO your_table (col1, col2) VALUES (%s, %s)", (3, 'C'))
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
