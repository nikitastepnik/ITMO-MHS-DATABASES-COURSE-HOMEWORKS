WITH
    RECURSIVE boss_counter(emp_id, boss_above_id, boss_above_count, steps) AS (
        SELECT emp_id, reports_to, 0, 1
        FROM staff
        UNION ALL
        SELECT
            emp_id,
            (SELECT reports_to FROM staff WHERE emp_id = boss_counter.boss_above_id AND reports_to IS NOT NULL),
            CASE WHEN boss_above_count < 20 THEN boss_above_count + 1 ELSE -1 END,
            steps + 1
        FROM boss_counter
        WHERE boss_above_id IS NOT NULL
    ),
    boss_counter_wrapper(emp_id, boss_above_count) AS (
        SELECT emp_id,
               CASE WHEN min_target_value = -1 THEN min_target_value ELSE  max_target_value END
               FROM
        (SELECT
            emp_id,
            MIN(boss_above_count) as min_target_value,
            MAX(boss_above_count) as max_target_value
        FROM boss_counter
        GROUP by emp_id
        ) inner_q
        ORDER BY emp_id
)
SELECT *
FROM boss_counter_wrapper
