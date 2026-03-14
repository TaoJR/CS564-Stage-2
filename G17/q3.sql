SELECT	
	d.department_name,
	AVG(j.max_salary) AS avg_max_salary
FROM departments d
JOIN employees e
	ON e.department_id = d.department_id
JOIN jobs j
	ON j.job_id = e.job_id
GROUP BY d.department_id, d.department_name
HAVING AVG(j.max_salary) > 8000;
