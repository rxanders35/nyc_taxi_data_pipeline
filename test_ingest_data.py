from ingest_data import read_csv, read_parquet, transform_columns, load_chunk_to_sql
import unittest
from unittest.mock import patch, Mock
from io import StringIO
import pandas as pd
import pyarrow.parquet as pq

class TestIngestData(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()
    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_read_csv(self):
        test_csv_data = pd.DataFrame({'column 1':[1, 3], 'column 2': [2, 4]})
        with patch('pd.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.read_csv(StringIO(test_csv_data))
            result = read_csv('fake_path.csv')
            self.assertEqual(len(result), 2)
            self.assertEqual(list(result.columns), ['col1', 'col2'])

    def test_read_parquet(self):
        test_parquet_data = pd.DataFrame({'column 1':[1, 3], 'column 2': [2, 4]})
        with patch('pq.read_table') as mock_read_table:
            mock_read_table.to_pandas.to_pandas = pd.read_csv(StringIO(test_parquet_data))
            result = read_parquet('fake_path.parquet')
            self.assertEqual(len(result), 2)
            self.assertEqual(list(result.columns), ['col1', 'col2'])