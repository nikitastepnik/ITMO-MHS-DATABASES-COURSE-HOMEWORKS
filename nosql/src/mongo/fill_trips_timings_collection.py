from pymongo import MongoClient

client = MongoClient('mongodb://root:pass@localhost:27017')
db = client['trips']
collection = db['trips_info']

pipeline = [
    {"$match": {"$expr": {"$gt": [{"$subtract": ["$tpep_dropoff_datetime",
                                                 "$tpep_pickup_datetime"]},
                                  60000]}}},
    {"$project": {"data": [
        {
            "trip_id": "$id",
            "VendorID": "$VendorID",
            "tpep_pickup_datetime": "$tpep_pickup_datetime",
            "PULocationID": "$PULocationID"
        },
        {
            "trip_id": "$id",
            "tpep_dropoff_datetime": "$tpep_dropoff_datetime",
            "passenger_count": "$passenger_count",
            "trip_distance": "$trip_distance",
            "RatecodeID": "$RatecodeID",
            "store_and_fwd_flag": "$store_and_fwd_flag",
            "DOLocationID": "$DOLocationID",
            "payment_type": "$payment_type",
            "fare_amount": "$fare_amount",
            "extra": "$extra",
            "mta_tax": "$mta_tax",
            "tip_amount": "$tip_amount",
            "tolls_amount": "$tolls_amount",
            "improvement_surcharge": "$improvement_surcharge",
            "total_amount": "$total_amount"
        }
    ]}},
    {"$unwind": "$data"},
    {"$sort": {"tpep_pickup_datetime": 1, "tpep_dropoff_datetime": 1}},
    {"$replaceRoot": {"newRoot": "$data"}},
    {"$out": "trips_timings"}
]

collection.aggregate(pipeline, allowDiskUse=True)
