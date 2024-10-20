import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

import numpy as np
import unittest
import pandas as pd
from fetch import clean_data, clean_duplicate, clean_position
from unittest.mock import patch

file_path = "./tests/files/aisdk-2024-09-09-mock.csv"
NUM_ROWS = len(pd.read_csv(file_path, sep=","))

class TestCleanData(unittest.TestCase):
    
    @patch('fetch.clean_duplicate') 
    @patch('fetch.clean_position') 
    def test_clean_data(self, mock_clean_position, mock_clean_duplicate):
        # Arrange
        test_data = pd.read_csv(file_path, sep=',')
        mock_clean_position.return_value = test_data
        mock_clean_duplicate.return_value = test_data
        
        # Act
        clean_data(test_data)
        
        # Assert
        mock_clean_duplicate.assert_called_once_with(test_data)
        mock_clean_position.assert_called_once_with(test_data)
    
    def test_clean_position_excludes_lat_over_90(self):
        # Arrange
        test_data = pd.read_csv(file_path, sep=',')
        
        # Act
        cleaned_data = clean_position(test_data)
        
        # Assert
        self.assertNotIn(91, cleaned_data['Latitude'].values)
        self.assertEqual(len(cleaned_data), NUM_ROWS-3) #3 rows removed because of lat > 90
    
    def test_clean_duplicate_excludes_duplicates(self):
        # Arrange
        test_data = pd.read_csv(file_path, sep=',')
        
        # Act
        cleaned_data = clean_duplicate(test_data)
        
        # Assert
        self.assertEqual(len(cleaned_data), NUM_ROWS-1) #1 row removed because of one duplicate 
    
if __name__ == '__main__':
    unittest.main()