SELECT COUNT(e.employee_id)
FROM departments d
JOIN employees e
	ON e.department_id = d.department_id
WHERE d.department_name = 'Shipping';
