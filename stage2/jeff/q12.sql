SELECT job_title, max_salary - AVG(salary) AS salary_gap
FROM jobs
JOIN employees ON jobs.job_id = employees.job_id
GROUP BY jobs.job_id, job_title, max_salary
ORDER BY salary_gap DESC;
