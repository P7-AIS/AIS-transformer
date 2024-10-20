# import sys
# import os
# sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

# import numpy as np
# import unittest
# import pandas as pd
# from fetch import ship_type_creator, ship_type_hashmap, clean_data
# from unittest.mock import MagicMock, patch

# file_path = "./tests/files/aisdk-2024-09-09-mock.csv"

# class TestShipTypeCreator(unittest.TestCase):

#     @patch('fetch.ship_type_hashmap')  # Mock ship_type_hashmap
#     def test_ship_type_creator(self, mock_ship_type_hashmap):
#         # Mock connection and cursor
#         mock_connection = MagicMock()
#         mock_cursor = mock_connection.cursor.return_value
        
#         # Mock the ship_type_hashmap return value
#         mock_ship_type_hashmap.return_value = {"Cargo": 1, "Tanker": 2}
        
#         # Arrange
#         ais_data = clean_data(pd.read_csv(file_path, sep=','))

#         # Act
#         result = ship_type_creator(mock_connection, ais_data)
        
#         # Assert that the SQL queries were executed as expected
#         mock_cursor.execute.assert_any_call("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM ship_type WITH NO DATA")
        
#         # Check that COPY was used to insert data into the temp table
#         mock_cursor.copy.return_value.write_row.assert_any_call(('Cargo',))
#         mock_cursor.copy.return_value.write_row.assert_any_call(('Tanker',))
        
#         # Assert that distinct ship types were inserted into ship_type table
#         mock_cursor.execute.assert_any_call("INSERT INTO ship_type (name) "
#                                             "SELECT DISTINCT tmp.name FROM tmp_table tmp "
#                                             "LEFT JOIN ship_type st ON tmp.name = st.name WHERE st.name IS NULL")
        
#         # Assert that commit was called
#         mock_connection.commit.assert_called_once()
        
#         # Assert that the cursor was closed
#         mock_cursor.close.assert_called_once()
        
#         # Assert that the ship_type_hashmap was called and the result is returned
#         mock_ship_type_hashmap.assert_called_once_with(mock_connection)
#         self.assertEqual(result, {"Cargo": 1, "Tanker": 2})
    
# if __name__ == '__main__':
#     unittest.main()