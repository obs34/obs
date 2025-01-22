# test_excel_reader.py
import unittest
import pandas as pd
from excel_reader import ExcelReader

class TestExcelReader(unittest.TestCase):
    def test_read(self):
        # Créer un DataFrame de test
        data = {'Col1': [1, 2, 3], 'Col2': ['A', 'B', 'C']}
        df = pd.DataFrame(data)

        # Sauvegarder le DataFrame dans un fichier Excel temporaire
        temp_file_path = 'test_file.xlsx'
        df.to_excel(temp_file_path, index=False)

        # Lire le fichier Excel avec ExcelReader
        reader = ExcelReader()
        result = reader.read(temp_file_path)

        # Vérifier que les données lues correspondent aux données attendues
        pd.testing.assert_frame_equal(result, df)

if __name__ == '__main__':
    unittest.main()
