SELECT COUNT(employee_id)
FROM departments
JOIN employees ON employees.department_id = departments.department_id
WHERE department_name = 'Shipping';
