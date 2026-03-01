SELECT country_name
FROM countries
JOIN regions on countries.region_id = regions.region_id
WHERE region_name = 'Europe';
