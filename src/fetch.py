import entity
import codecs
from enum import Enum
import pandas as pd
import numpy as np

class CsvAtt(Enum):
    TIMESTAMP = 0
    TYPE_OF_MOBILE = 1
    MMSI = 2
    LATITUDE = 3
    LONGITUDE = 4
    NAVIGATIONAL_STATUS = 5
    ROT = 5
    SOG = 6
    COG = 7
    HEADING = 8
    IMO = 9
    CALLSIGN = 10
    NAME = 11
    SHIP_TYPE = 12
    CARGO_TYPE = 13
    WIDTH = 14
    LENGTH = 15
    TYPE_OF_POSITION_FIXING_DEVICE = 16
    DRAUGHT = 17
    DESTINATION = 18
    ETA = 19
    DATA_SOURCE_TYPE = 20
    A = 20
    B = 21
    C = 22
    D = 23


def read_files(connection, path: str):
    ais_data = parse_csv(path)
    clean_ais_data = clean_data(ais_data)
    destination = destination_creator(connection, ais_data)
    ship_type = ship_type_creator(connection, ais_data)
    mobile_type = mobile_type_creator(connection, ais_data)
    navigational_status = navigational_status_creator(connection, ais_data)
    vessel = vessel_creator(connection, ais_data, ship_type)


def parse_csv(file):
    return pd.read_csv(file, compression='zip', sep=',')

def clean_data(ais_data):
    data = clean_position(ais_data)
    data = clean_duplicate(data)
    return data

def clean_position(ais_data):
    return ais_data[ais_data['Latitude'] <= 90]

def clean_duplicate(ais_data):
    return ais_data.drop_duplicates()

def destination_creator(connection, ais_data):
    destinations = ais_data['Destination'].unique()

    # Start a database transaction.
    cur = connection.cursor()

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM destination WITH NO DATA")
    with cur.copy("COPY tmp_table (name) FROM STDIN") as copy:
        for destination in destinations.tolist():
            copy.write_row((destination,))

    cur.execute("INSERT INTO destination (name) SELECT DISTINCT tmp.name FROM tmp_table tmp LEFT JOIN destination dest ON tmp.name = dest.name WHERE dest.name IS NULL")

    connection.commit()
    cur.close()

    return destination_hashmap(connection)

def destination_hashmap(conn):
    destination_hash = {}

    cur = conn.cursor()

    with cur.copy("COPY destination (id, name) TO STDOUT") as copy:
        copy.set_types(["int8", "text"])
        for row in copy.rows():
            destination_hash[row[1]] = row[0]

    cur.close()

    return destination_hash

def ship_type_creator(connection, ais_data):
    ship_types = ais_data['Ship type'].unique()

    cur = connection.cursor()

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM ship_type WITH NO DATA")
    with cur.copy("COPY tmp_table (name) FROM STDIN") as copy:
        for ship_type in ship_types.tolist():
            copy.write_row((ship_type,))

    cur.execute("INSERT INTO ship_type (name) SELECT DISTINCT tmp.name FROM tmp_table tmp LEFT JOIN ship_type st ON tmp.name = st.name WHERE st.name IS NULL")

    connection.commit()
    cur.close()

    return ship_type_hashmap(connection)

def ship_type_hashmap(conn):
    ship_type_hash = {}

    cur = conn.cursor()

    with cur.copy("COPY ship_type (id, name) TO STDOUT") as copy:
        copy.set_types(["int8", "text"])
        for row in copy.rows():
            ship_type_hash[row[1]] = row[0]

    cur.close()

    return ship_type_hash

def mobile_type_creator(connection, ais_data):
    mobile_types = ais_data['Type of mobile'].unique()

    cur = connection.cursor()

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM mobile_type WITH NO DATA")
    with cur.copy("COPY tmp_table (name) FROM STDIN") as copy:
        for mobile_type in mobile_types.tolist():
            copy.write_row((mobile_type,))

    cur.execute("INSERT INTO mobile_type (name) SELECT DISTINCT tmp.name FROM tmp_table tmp LEFT JOIN mobile_type mt ON tmp.name = mt.name WHERE mt.name IS NULL")

    connection.commit()
    cur.close()

    return mobile_type_hashmap(connection)

def mobile_type_hashmap(conn):
    mobile_type_hash = {}

    cur = conn.cursor()

    with cur.copy("COPY mobile_type (id, name) TO STDOUT") as copy:
        copy.set_types(["int8", "text"])
        for row in copy.rows():
            mobile_type_hash[row[1]] = row[0]

    cur.close()

    return mobile_type_hash

def navigational_status_creator(connection, ais_data):
    navigational_statuses = ais_data['Navigational status'].unique()

    cur = connection.cursor()

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM navigational_status WITH NO DATA")
    with cur.copy("COPY tmp_table (name) FROM STDIN") as copy:
        for navigational_status in navigational_statuses.tolist():
            copy.write_row((navigational_status,))

    cur.execute("INSERT INTO navigational_status (name) SELECT DISTINCT tmp.name FROM tmp_table tmp LEFT JOIN navigational_status ns ON tmp.name = ns.name WHERE ns.name IS NULL")

    connection.commit()
    cur.close()

    return navigational_status_hashmap(connection)

def navigational_status_hashmap(conn):
    navigational_status_hash = {}

    cur = conn.cursor()

    with cur.copy("COPY navigational_status (id, name) TO STDOUT") as copy:
        copy.set_types(["int8", "text"])
        for row in copy.rows():
            navigational_status_hash[row[1]] = row[0]

    cur.close()

    return navigational_status_hash

def vessel_creator(connection, ais_data, ship_type):
    vessels = ais_data[['MMSI', 'Name', 'Ship type', 'IMO', 'Callsign', 'Width', 'Length', 'Type of position fixing device', 'A', 'B', 'C', 'D']]
    vessels['IMO'] = vessels['IMO'].replace('Unknown', None)
    vessels = vessels.replace(np.nan, None)


    print(vessels)

    cur = connection.cursor()

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM vessel WITH NO DATA")
    with cur.copy("COPY tmp_table (mmsi, name, ship_type_id, imo, call_sign, width, length, position_fixing_device, to_bow, to_stern, to_port, to_starboard) FROM STDIN") as copy:
        for i in range(0,len(vessels.index)):
            mmsi = vessels['MMSI'][i]
            name = vessels['Name'][i]
            st = ship_type[vessels['Ship type'][i]]
            imo = vessels['IMO'][i]
            call_sign = vessels['Callsign'][i]
            width = int(vessels['Width'][i]) if not pd.isna(vessels['Width'][i]) else None
            length = int(vessels['Length'][i]) if not pd.isna(vessels['Length'][i]) else None
            topfd = vessels['Type of position fixing device'][i]
            a = int(vessels['A'][i]) if not pd.isna(vessels['A'][i]) else None
            b = int(vessels['B'][i]) if not pd.isna(vessels['B'][i]) else None
            c = int(vessels['C'][i]) if not pd.isna(vessels['C'][i]) else None
            d = int(vessels['D'][i]) if not pd.isna(vessels['D'][i]) else None
            print((mmsi, name, st, imo, call_sign, width, length, topfd, a, b, c, d))
            copy.write_row((mmsi, name, st, imo, call_sign, width, length, topfd, a, b, c, d))

    cur.execute("INSERT INTO vessel mmsi, name, ship_type_id, imo, call_sign, width, length, position_fixing_device, to_bow, to_stern, to_port, to_starboard SELECT DISTINCT tmp.mmsi FROM tmp_table tmp LEFT JOIN vessel vl ON tmp.mmsi = vl.mmsi WHERE vl.mmsi IS NULL")

    connection.commit()
    cur.close()
