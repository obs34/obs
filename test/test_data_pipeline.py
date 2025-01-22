# test_data_pipeline.py
import unittest
from unittest.mock import MagicMock
from data_pipeline import DataPipeline

class TestDataPipeline(unittest.TestCase):
    def test_run(self):
        # Créer des mocks pour les dépendances
        excel_reader = MagicMock()
        data_processor = MagicMock()
        database_writer = MagicMock()
        logger = MagicMock()

        # Configurer les retours des mocks
        excel_reader.read.return_value = 'data'
        data_processor.process.return_value = 'processed_data'

        # Créer une instance de DataPipeline avec les mocks
        pipeline = DataPipeline(excel_reader, data_processor, database_writer, logger)

        # Exécuter le pipeline
        pipeline.run('fake_file_path')

        # Vérifier que les méthodes des mocks ont été appelées correctement
        excel_reader.read.assert_called_with('fake_file_path')
        data_processor.process.assert_called_with('data')
        database_writer.write.assert_called_with('processed_data')
        logger.log.assert_any_call("Données lues depuis fake_file_path")
        logger.log.assert_any_call("Données traitées")
        logger.log.assert_any_call("Données versées dans la base de données")

if __name__ == '__main__':
    unittest.main()
