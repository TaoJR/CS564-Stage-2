SELECT
	d.department_name,
	COUNT(e.employee_id) AS num_employees
FROM departments d
LEFT JOIN employees e
	ON e.department_id = d.department_id
GROUP BY d.department_id, d.department_name
ORDER BY num_employees DESC, d.department_name ASC;
