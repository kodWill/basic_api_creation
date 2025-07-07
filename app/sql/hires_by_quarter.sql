WITH hires_by_quarter AS (
    SELECT
        d.department,
        j.job,
        EXTRACT(QUARTER FROM CAST(datetime AS DATE)) AS quarter
    FROM employees AS e
    JOIN departments AS d
        ON e.department_id = d.id
    JOIN jobs AS j
        ON e.job_id = j.id
    WHERE EXTRACT(YEAR FROM CAST(datetime AS DATE)) = :year AND e.datetime != 'NaN'
        
)

SELECT
    department,
    job,
    COUNT(1) FILTER (WHERE quarter = '1') AS q1,
    COUNT(1) FILTER (WHERE quarter = '2') AS q2,
    COUNT(1) FILTER (WHERE quarter = '3') AS q3,
    COUNT(1) FILTER (WHERE quarter = '4') AS q4
FROM hires_by_quarter
GROUP BY 
    department,
    job
ORDER BY 
    department,
    job
;
