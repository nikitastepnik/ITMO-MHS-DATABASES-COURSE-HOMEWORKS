CREATE DATABASE taxi;

CREATE TABLE trips (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER DEFAULT 0,
    trip_distance DOUBLE PRECISION DEFAULT 0.0,
    RatecodeID INTEGER,
    store_and_fwd_flag CHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION DEFAULT 0.0,
    extra DOUBLE PRECISION DEFAULT 0.0,
    mta_tax NUMERIC DEFAULT 0.0,
    tip_amount NUMERIC DEFAULT 0.0,
    tolls_amount NUMERIC DEFAULT 0.0,
    improvement_surcharge NUMERIC DEFAULT 0.0,
    total_amount NUMERIC DEFAULT 0.0
);
