SELECT department_name, AVG(jobs.max_salary)
FROM departments
JOIN employees ON departments.department_id = employees.department_id
JOIN jobs ON employees.job_id = jobs.job_id
GROUP BY departments.department_id, departments.department_name
HAVING AVG(jobs.max_salary) > 8000;
