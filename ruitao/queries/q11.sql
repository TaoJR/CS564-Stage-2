SELECT DISTINCT
	e.first_name,
	e.last_name
FROM employees e
LEFT JOIN jobs j
  ON j.job_id = e.job_id
WHERE j.job_title LIKE '%Manager%'
   OR e.employee_id IN (
        SELECT manager_id
        FROM employees
        WHERE manager_id IS NOT NULL
      );
