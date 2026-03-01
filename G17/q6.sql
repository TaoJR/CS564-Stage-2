SELECT COUNT(employee_id)
FROM employees
JOIN departments ON employees.department_id = departments.department_id
JOIN locations ON departments.location_id = locations.location_id
JOIN countries ON locations.country_id = countries.country_id
JOIN regions ON countries.region_id = regions.region_id
WHERE region_name = 'Europe';
