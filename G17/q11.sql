SELECT first_name, last_name
FROM employees
JOIN jobs ON employees.job_id = jobs.job_id
WHERE job_title LIKE '%Manager%' 
  OR employee_id IN(
  SELECT manager_id
  FROM employees
  WHERE manager_id IS NOT NULL
  );
