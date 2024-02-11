WITH
    RECURSIVE euclidean_algorithm_cte(a, b, a_init, b_init) AS (
        SELECT a, b, a, b
        FROM nums
        UNION ALL
        SELECT
        CASE WHEN a = 0 THEN b WHEN a > b THEN a-b ELSE a END,
        CASE WHEN b = 0 THEN a WHEN b > a THEN b-a ELSE b END,
        a_init,
        b_init
        FROM euclidean_algorithm_cte WHERE a != b
    ),
    euclidean_algorithm_wrapper(a, b) AS (
        SELECT DISTINCT
            a_init,
            b_init,
            CASE
            WHEN a_init != 0 AND b_init != 0 THEN
              (MIN(a) OVER (PARTITION BY a_init, b_init))
            ELSE
              GREATEST(a_init, b_init)
            END
                AS gcd
        FROM euclidean_algorithm_cte
)
SELECT a, b, gcd
FROM euclidean_algorithm_wrapper
