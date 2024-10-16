from pydantic import BaseModel
from decimal import *
from datetime import datetime, date, time, timezone

class AIS_Message:
    id: int
    vessel_mmsi: int
    destination_id: int
    mobile_type_id: int
    navigational_status_id: int
    data_source_type: str
    timestamp: str
    latitude: Decimal
    longitude: Decimal
    rot: Decimal
    sog: Decimal
    heading: int
    draught: Decimal
    cargo_type: str
    eta: str

class Vessel:
    mmsi: int
    name: str
    ship_type_id: int
    imo: int
    call_sign: str
    flag: str
    width: int
    length: int
    position_fixing_device: str
    to_bow: int
    to_stern: int
    to_port: int
    to_starboard: int

class Destination:
    id: int
    name: str

class Mobile_Type:
    id: int
    name: str

class Navigational_Status:
    id: int
    name: str

class DbData:
    message: list[AIS_Message]
    vessel: list[Vessel]
    destination: list[Destination]
    mobile_type: list[Mobile_Type]
    navigational_status: list[Navigational_Status]



