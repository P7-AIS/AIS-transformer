import entity
import codecs
from enum import Enum
import pandas as pd
import numpy as np
from datetime import datetime

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
    print("read csv")
    clean_ais_data = clean_data(ais_data)
    print("cleaned dataset")
    ship_type = ship_type_creator(connection, clean_ais_data.copy())
    print("uploaded ship type")
    mobile_type = mobile_type_creator(connection, clean_ais_data.copy())
    print("uploaded mobile type")
    navigational_status = navigational_status_creator(connection, clean_ais_data.copy())
    print("uploaded navigational status")
    countryIds = country_creator(connection, ais_data)
    print("uploaded countries")
    vessel = vessel_creator(connection, clean_ais_data.copy(), ship_type, countryIds)
    print("uploaded vessels")
    ais_message_creator(connection, clean_ais_data.copy(), mobile_type, navigational_status)
    print("uploaded ais messages")
    vessel_trajectory_creator(connection, clean_ais_data.copy())
    print("uploaded vessel trajectories")

def parse_csv(file):
    return pd.read_csv(file, compression='zip', sep=',')

def clean_data(ais_data):
    data = clean_position(ais_data)
    data = clean_duplicate(data)
    data = data.rename(columns={'# Timestamp': 'Timestamp'})
    data = data.replace(np.nan, None)
    return data

def clean_position(ais_data): #Removes all rows where the latitude is above 90 (GPS failure, or is it?)
    return ais_data[ais_data['Latitude'] <= 90]

def clean_duplicate(ais_data):
    return ais_data.drop_duplicates()

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

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM nav_status WITH NO DATA")
    with cur.copy("COPY tmp_table (name) FROM STDIN") as copy:
        for navigational_status in navigational_statuses.tolist():
            copy.write_row((navigational_status,))

    cur.execute("INSERT INTO nav_status (name) SELECT DISTINCT tmp.name FROM tmp_table tmp LEFT JOIN nav_status ns ON tmp.name = ns.name WHERE ns.name IS NULL")

    connection.commit()
    cur.close()

    return navigational_status_hashmap(connection)

def navigational_status_hashmap(conn):
    navigational_status_hash = {}

    cur = conn.cursor()

    with cur.copy("COPY nav_status (id, name) TO STDOUT") as copy:
        copy.set_types(["int8", "text"])
        for row in copy.rows():
            navigational_status_hash[row[1]] = row[0]

    cur.close()

    return navigational_status_hash

def country_creator(connection, ais_data):
    mmsi = ais_data['MMSI'].unique()
    file_path = "./files/countries.xlsx"
    countries_df = pd.read_excel(file_path)
    
    countries_df.drop_duplicates(subset="Digit", keep='first', inplace=True)

    cur = connection.cursor()
    
    countryIds = list(countries_df['Digit'].astype(str))

    # print(countryIds)
    
    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM mid WITH NO DATA")
    with cur.copy("COPY tmp_table (id, country, country_short) FROM STDIN") as copy:
        for i in range(0,len(countries_df.index)):
            id = countries_df['Digit'].iloc[i]
            country = countries_df['Allocated to'].iloc[i]
            country_short = None
            copy.write_row((id, country, country_short))

    cur.execute("""INSERT INTO mid (id, country, country_short) 
                SELECT DISTINCT 
                    tmp.id, 
                    tmp.country, 
                    tmp.country_short 
                FROM tmp_table tmp LEFT JOIN mid ON tmp.id = mid.id
                WHERE mid.id IS NULL""")

    connection.commit()
    cur.close()

    return countryIds

def vessel_creator(connection, ais_data, ship_type, countryIDs):
    vessels = ais_data[['MMSI', 'Name', 'Ship type', 'IMO', 'Callsign', 'Width', 'Length', 'Type of position fixing device', 'A', 'B', 'C', 'D']].groupby('MMSI', as_index=False).first()
    vessels['IMO'] = vessels['IMO'].replace('Unknown', None)
    vessels = vessels.replace(np.nan, None)

    cur = connection.cursor()

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM vessel WITH NO DATA")
    with cur.copy("COPY tmp_table (mmsi, name, ship_type_id, imo, call_sign, width, length, position_fixing_device, to_bow, to_stern, to_port, to_starboard, country_id) FROM STDIN") as copy:
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
            country_id = str(mmsi)[:3] if str(mmsi)[:3] in countryIDs else None
            
            copy.write_row((mmsi, name, st, imo, call_sign, width, length, topfd, a, b, c, d, country_id))

    cur.execute("""INSERT INTO vessel (mmsi, name, ship_type_id, imo, call_sign, width, length, position_fixing_device, to_bow, to_stern, to_port, to_starboard, country_id)
                SELECT DISTINCT
                    tmp.mmsi,
                    tmp.name,
                    tmp.ship_type_id,
                    tmp.imo,
                    tmp.call_sign,
                    tmp.width,
                    tmp.length,
                    tmp.position_fixing_device,
                    tmp.to_bow,
                    tmp.to_stern,
                    tmp.to_port,
                    tmp.to_starboard,
                    tmp.country_id
                FROM tmp_table tmp LEFT JOIN vessel vl ON tmp.mmsi = vl.mmsi
                WHERE vl.mmsi IS NULL""")

    connection.commit()
    cur.close()

def ais_message_creator(connection, ais_data, mobile_types, navigational_statuses):
    ais_messages = ais_data[['Timestamp', 'Type of mobile', 'MMSI', 'Latitude', 'Longitude', 'Navigational status', 'ROT', 'SOG', 'COG', 'Heading', 'Cargo type', 'Draught', 'Destination', 'ETA', 'Data source type']]

    cur = connection.cursor()

    # print(ais_messages)

    cur.execute("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM ais_message WITH NO DATA")
    with cur.copy("COPY tmp_table (destination, mobile_type_id, nav_status_id, data_source_type, timestamp, rot, sog, cog, heading, draught, cargo_type, eta, vessel_mmsi) FROM STDIN") as copy:
        for i in range(0,len(ais_messages.index)):
            destination = ais_messages['Destination'].iloc[i]
            mobile_type_id = mobile_types[ais_messages['Type of mobile'].iloc[i]]
            nav_status_id = navigational_statuses[ais_messages['Navigational status'].iloc[i]]
            data_source_type = ais_messages['Data source type'].iloc[i]
            timestamp = datetime.strptime(ais_messages['Timestamp'].iloc[i], "%d/%m/%Y %H:%M:%S")
            rot = ais_messages['ROT'].iloc[i]
            sog = ais_messages['SOG'].iloc[i]
            cog = ais_messages['COG'].iloc[i]
            heading = int(ais_messages['Heading'].iloc[i]) if not pd.isna(ais_messages['Heading'].iloc[i]) else None
            draught = ais_messages['Draught'].iloc[i]
            cargo_type = ais_messages['Cargo type'].iloc[i]
            eta = datetime.strptime(ais_messages['ETA'].iloc[i], "%d/%m/%Y %H:%M:%S") if not pd.isna(ais_messages['ETA'].iloc[i]) else None
            vessel_mmsi = ais_messages['MMSI'].iloc[i]

            copy.write_row((destination, mobile_type_id, nav_status_id, data_source_type, timestamp, rot, sog, cog, heading, draught, cargo_type, eta, vessel_mmsi))

    cur.execute("""INSERT INTO ais_message (destination, mobile_type_id, nav_status_id, data_source_type, timestamp, rot, sog, cog, heading, draught, cargo_type, eta, vessel_mmsi)
                SELECT DISTINCT tmp.destination,
                    tmp.mobile_type_id,
                    tmp.nav_status_id,
                    tmp.data_source_type,
                    tmp.timestamp,
                    tmp.rot,
                    tmp.sog,
                    tmp.cog,
                    tmp.heading,
                    tmp.draught,
                    tmp.cargo_type,
                    tmp.eta,
                    tmp.vessel_mmsi
                FROM tmp_table tmp""")

    connection.commit()
    cur.close()


def vessel_trajectory_creator(connection, ais_data):
    traj_ais_data = ais_data[['Timestamp', 'MMSI', 'Latitude', 'Longitude']]
    mmsis = traj_ais_data['MMSI'].unique()

    for mmsi in mmsis:
        vessel_ais_data = traj_ais_data.query(f"MMSI == {mmsi}")    
        single_vessel_trajectory(connection, mmsi, vessel_ais_data)
    

def single_vessel_trajectory(connection, mmsi, vessel_ais_data):
    cur = connection.cursor()
    cur.execute("""CREATE TEMP TABLE tmp_table (
                    MMSI BIGINT,
                    timestamp TIMESTAMP,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION
                ) ON COMMIT DROP;""")

    with cur.copy("COPY tmp_table (mmsi, timestamp, latitude, longitude) FROM STDIN") as copy:
        for i in range(len(vessel_ais_data.index)):
            timestamp = datetime.strptime(vessel_ais_data['Timestamp'].iloc[i], "%d/%m/%Y %H:%M:%S")
            latitude = vessel_ais_data['Latitude'].iloc[i]
            longitude = vessel_ais_data['Longitude'].iloc[i]
            copy.write_row((mmsi, timestamp, latitude, longitude))

    # Insert the MMSI into the vessel_trajectory table if it doesn't exist
    insert_mmsi_query = f"""
        INSERT INTO vessel_trajectory (mmsi, trajectory)
        VALUES ({mmsi}, ST_SetSRID(ST_GeomFromText('LINESTRINGM EMPTY'), 4326))  -- Empty M dimension linestring with SRID 4326
        ON CONFLICT (mmsi) DO NOTHING;  -- Prevent duplicate entries if MMSI already exists
    """
    cur.execute(insert_mmsi_query)

    merge_query = f"""
        WITH new_trajectory AS (
            SELECT ST_SetSRID(
                ST_MakeLine(
                    ST_MakePointM(longitude, latitude, EXTRACT(EPOCH FROM timestamp)) 
                    ORDER BY timestamp
                ), 4326) AS new_geom  -- Set SRID to 4326 for new geometries
            FROM tmp_table
            WHERE mmsi = {mmsi}  -- Correctly filtering by MMSI
        ),
        existing_trajectory AS (
            SELECT ST_SetSRID(trajectory, 4326) AS trajectory  -- Ensure the existing geometry has the SRID 4326
            FROM vessel_trajectory 
            WHERE mmsi = {mmsi}  -- Ensure this matches the vessel identifier
        )
        UPDATE vessel_trajectory
        SET trajectory = ST_Union(existing_trajectory.trajectory, new_trajectory.new_geom)
        FROM new_trajectory, existing_trajectory
        WHERE vessel_trajectory.mmsi = {mmsi};  -- Ensuring correct reference
    """
    cur.execute(merge_query)

    connection.commit()
    cur.close()

    # print(vessel_ais_data)
    
    # Construct empty linestring for 


