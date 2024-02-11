SELECT vendorid,
       AVG(trip_distance)                             AS avg_trip_distance,
       VAR_SAMP(total_amount)                         AS trip_amount_despersion,
       SUM(CASE
               WHEN date_part('hour', tpep_pickup_datetime) BETWEEN 6 AND 17
                   THEN 1
               ELSE 0
           END)                                       AS "6am_to_6pm_count_trips",
       SUM(CASE
               WHEN (date_part('hour', tpep_pickup_datetime) = 23
                         AND EXTRACT(DAY
                                     FROM tpep_pickup_datetime) = 31
                         AND EXTRACT(MONTH
                                     FROM tpep_pickup_datetime) = 12
                   OR date_part('hour', tpep_dropoff_datetime) = 0
                         AND EXTRACT(DAY
                                     FROM tpep_dropoff_datetime) = 1
                         AND EXTRACT(MONTH
                                     FROM tpep_dropoff_datetime) = 1) THEN 1
               ELSE 0
           END)                                       AS new_year_in_trip,
       MAX(total_amount / NULLIF(passenger_count, 0)) AS max_average_cost_per_passenger
FROM trips
GROUP BY vendorid
