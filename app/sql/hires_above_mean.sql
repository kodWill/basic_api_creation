WITH hires_by_department AS (
    SELECT
        d.id,
        d.department,
        COUNT(1) AS hires
    FROM employees AS e
    JOIN departments AS d
        ON e.department_id = d.id
    JOIN jobs AS j
        ON e.job_id = j.id
    WHERE EXTRACT(YEAR FROM CAST(datetime AS DATE)) = :year AND e.datetime != 'NaN'
    GROUP BY 
        d.id,
        d.department
)
SELECT 
    id,
    department,
    hires AS hired
FROM hires_by_department
WHERE hires > (SELECT AVG(hires) FROM hires_by_department)
ORDER BY hires DESC;