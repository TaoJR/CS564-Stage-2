SELECT department_name
FROM departments
JOIN employees ON employees.department_id = departments.department_id
WHERE employees.salary = (SELECT MAX(salary) FROM employees);
