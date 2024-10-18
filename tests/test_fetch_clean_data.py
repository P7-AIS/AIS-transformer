import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

import unittest
import pandas as pd
from fetch import clean_data

class TestCleanData(unittest.TestCase):
    
    def test_clean_data_excludes_lat_over_90(self):
        # Simulate some sample data
        test_data = pd.DataFrame({
            'Timestamp': ['09/09/2024 00:00:00', '09/09/2024 00:00:00'],
            'Latitude': [55.032667, 91.0],  # Invalid latitude
            'MMSI': [2190067, 219008417],
            'Ship type': ['Undefined', 'Undefined'],
            'Navigational status': ['Unknown value', 'Under way using engine']
        })
        
        cleaned_data = clean_data(test_data)

        # Check if the invalid latitude was removed
        self.assertNotIn(91.0, cleaned_data['Latitude'].values)
        self.assertEqual(len(cleaned_data), 1)  # One valid entry left

if __name__ == '__main__':
    unittest.main()