SELECT trip_id,
       ROUND(tip_amount / NULLIF(total_amount, 0),
             4)   AS tip_part_in_total_amount,
       ROUND(tip_amount / NULLIF(total_amount, 0) -
             (SELECT AVG(tip_amount / NULLIF(total_amount, 0))
              FROM trips),
             4)   AS diff_between_tip_parts_global_sample,
       ROUND(tip_amount / NULLIF(total_amount, 0) -
             AVG(tip_amount / NULLIF(total_amount, 0))
             OVER (PARTITION BY vendorid),
             4)   AS diff_between_tip_parts_vendor_sample,
       ROUND(
               (SELECT diff
                FROM (SELECT DISTINCT l_trips.trip_id,
                                      ROUND(l_trips.tip_amount /
                                            NULLIF(l_trips.total_amount, 0) -
                                            AVG(r_trips.tip_amount /
                                                NULLIF(r_trips.total_amount, 0))
                                            OVER (PARTITION BY l_trips.trip_id),
                                            4) AS diff
                      FROM trips AS l_trips
                               JOIN trips AS r_trips ON EXTRACT(DAY
                                                                FROM
                                                                l_trips.tpep_pickup_datetime) =
                                                        EXTRACT(DAY
                                                                FROM
                                                                r_trips.tpep_pickup_datetime)
                          AND l_trips.trip_distance < r_trips.trip_distance) q
                WHERE trips.trip_id = q.trip_id),
               4) AS diff_between_tip_parts_longer_trips_sample,
       (CASE
            WHEN CAST(trip_distance / NULLIF(EXTRACT(EPOCH
                                                     FROM
                                                     (tpep_dropoff_datetime - tpep_pickup_datetime) /
                                                     60.0),
                                             0) AS double precision) -
                 CAST(trip_distance / NULLIF(AVG(EXTRACT(EPOCH
                                                         FROM
                                                         (tpep_dropoff_datetime - tpep_pickup_datetime) /
                                                         60.0))
                                             OVER (PARTITION BY vendorid),
                                             0) AS double precision) > 0 THEN 1
            ELSE 0
           END)   AS speed_rank
FROM trips
