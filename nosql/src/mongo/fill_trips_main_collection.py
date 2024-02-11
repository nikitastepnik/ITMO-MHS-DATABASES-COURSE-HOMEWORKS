import csv
import dataclasses
import datetime
import typing

from pymongo import MongoClient


@dataclasses.dataclass
class Trip:
    id: int
    VendorID: int
    tpep_pickup_datetime: datetime.datetime
    tpep_dropoff_datetime: datetime.datetime
    passenger_count: int
    trip_distance: float
    RatecodeID: int
    store_and_fwd_flag: bool
    PULocationID: int
    DOLocationID: int
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float


def convert_to_int(value) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


def convert_to_float(value) -> float:
    try:
        return float(value)
    except ValueError:
        return 0.0


def convert_to_datetime(value) -> typing.Optional[datetime.datetime]:
    try:
        return datetime.datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        return None


client = MongoClient('mongodb://root:pass@localhost:27017')
db = client['trips']

collection = db['trips_info']

with open('./trips_info.csv', 'r') as file:
    reader = csv.DictReader(file)
    for idx, row in enumerate(reader):
        trip = Trip(
            id=idx,
            VendorID=convert_to_int(row['VendorID']),
            tpep_pickup_datetime=convert_to_datetime(
                row['tpep_pickup_datetime']),
            tpep_dropoff_datetime=convert_to_datetime(
                row['tpep_dropoff_datetime']),
            passenger_count=convert_to_int(row['passenger_count']),
            trip_distance=convert_to_float(row['trip_distance']),
            RatecodeID=convert_to_int(row['RatecodeID']),
            store_and_fwd_flag=True if row['store_and_fwd_flag'] == 'Y'
            else False,
            PULocationID=convert_to_int(row['PULocationID']),
            DOLocationID=convert_to_int(row['DOLocationID']),
            payment_type=convert_to_int(row['payment_type']),
            fare_amount=convert_to_float(row['fare_amount']),
            extra=convert_to_float(row['extra']),
            mta_tax=convert_to_float(row['mta_tax']),
            tip_amount=convert_to_float(row['tip_amount']),
            tolls_amount=convert_to_float(row['tolls_amount']),
            improvement_surcharge=convert_to_float(
                row['improvement_surcharge']),
            total_amount=convert_to_float(row['total_amount']),
        )
        collection.insert_one(trip.__dict__)
