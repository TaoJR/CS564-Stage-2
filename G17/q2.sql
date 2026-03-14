SELECT department_name, COUNT(employee_id) AS num_employees
FROM departments
JOIN employees ON departments.department_id = employees.department_id
GROUP BY departments.department_id, department_name
ORDER BY num_employees DESC;
