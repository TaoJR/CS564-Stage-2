.print ""
.print "Query 1: Job titles with max_salary >= 5000 and min_salary <= 4000"
SELECT DISTINCT job_title 
FROM jobs 
WHERE max_salary >= 5000 AND min_salary <= 4000;

.print ""
.print "Query 2: Department name and employee count, ordered by count DESC"
SELECT d.department_name, COUNT(e.employee_id) as num_employees
FROM departments d
JOIN employees e ON d.department_id = e.department_id
GROUP BY d.department_id, d.department_name
ORDER BY num_employees DESC;

.print ""
.print "Query 3: Departments with average max_salary > 8000"
SELECT d.department_name, AVG(j.max_salary) as avg_max_salary
FROM departments d
JOIN employees e ON d.department_id = e.department_id
JOIN jobs j ON e.job_id = j.job_id
GROUP BY d.department_id, d.department_name
HAVING AVG(j.max_salary) > 8000;

.print ""
.print "Query 4: Number of employees in 'Shipping' department"
SELECT COUNT(*) as num_employees
FROM employees e
JOIN departments d ON e.department_id = d.department_id
WHERE d.department_name = 'Shipping';

.print ""
.print "Query 5: Names of all countries in 'Europe' region"
SELECT c.country_name
FROM countries c
JOIN regions r ON c.region_id = r.region_id
WHERE r.region_name = 'Europe';

.print ""
.print "Query 6: Number of employees in 'Europe' region"
SELECT COUNT(*) as num_employees
FROM employees e
JOIN departments d ON e.department_id = d.department_id
JOIN locations l ON d.location_id = l.location_id
JOIN countries c ON l.country_id = c.country_id
JOIN regions r ON c.region_id = r.region_id
WHERE r.region_name = 'Europe';

.print ""
.print "Query 7: Pairs of employees (first names) in same dept, same manager, salary > 10000"
SELECT e1.first_name, e2.first_name
FROM employees e1
JOIN employees e2 ON e1.department_id = e2.department_id 
    AND e1.manager_id = e2.manager_id
WHERE e1.salary > 10000 
    AND e2.salary > 10000 
    AND e1.salary >= e2.salary 
    AND e1.employee_id != e2.employee_id;

.print ""
.print "Query 8: Manager ID and salary of employee with lowest salary"
SELECT manager_id, salary
FROM employees
ORDER BY salary ASC
LIMIT 1;

.print ""
.print "Query 9: Department name with highest paid employee"
SELECT d.department_name
FROM departments d
JOIN employees e ON d.department_id = e.department_id
ORDER BY e.salary DESC
LIMIT 1;

.print ""
.print "Query 10: Employee IDs with no dependents"
SELECT e.employee_id
FROM employees e
WHERE e.employee_id NOT IN (
    SELECT DISTINCT employee_id FROM dependents
);

.print ""
.print "Query 11: Names (first and last) of employees who are managers"
SELECT DISTINCT e.first_name, e.last_name
FROM employees e
WHERE e.employee_id IN (
    SELECT DISTINCT manager_id 
    FROM employees 
    WHERE manager_id IS NOT NULL
);

.print ""
.print "Query 12: Job title and salary gap (max_salary - avg current salary)"
SELECT j.job_title, (j.max_salary - AVG(e.salary)) as salary_gap
FROM jobs j
JOIN employees e ON j.job_id = e.job_id
GROUP BY j.job_id, j.job_title
ORDER BY salary_gap DESC;
