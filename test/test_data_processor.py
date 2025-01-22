# test_data_processor.py
import unittest
import pandas as pd
from data_processor import DataProcessor

class TestDataProcessor(unittest.TestCase):
    def test_process(self):
        # Créer un DataFrame de test
        data = {'Col1': [1, 2, 3], 'Col2': ['A', 'B', 'C']}
        df = pd.DataFrame(data)

        # Traiter les données avec DataProcessor
        processor = DataProcessor()
        result = processor.process(df)

        # Vérifier que les données transformées correspondent aux données attendues
        expected_data = {'Col1': [1, 2, 3], 'Col2': ['A', 'B', 'C'], 'NewCol': [2, 4, 6]}
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result, expected_df)

if __name__ == '__main__':
    unittest.main()
