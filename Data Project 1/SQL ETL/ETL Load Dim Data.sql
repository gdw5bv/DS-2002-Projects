/* Extracting appropriate data from humanresources database, and INSERTing into the datawarehouse - humanresources_dw database. */
/* Performing ETL through SQL. */

/* Populate dim_employees */
INSERT INTO `humanresources_dw`.`dim_employees`
(`employee_key`,
`first_name`,
`last_name`,
`email`,
`phone_number`,
`hire_date`,
`job_key`,
`salary`,
`manager_key`,
`department_key`)
SELECT `employees`.`employee_id`,
    `employees`.`first_name`,
    `employees`.`last_name`,
    `employees`.`email`,
    `employees`.`phone_number`,
    `employees`.`hire_date`,
    `employees`.`job_id`,
    `employees`.`salary`,
    `employees`.`manager_id`,
    `employees`.`department_id`
FROM `humanresources`.`employees`;

/* Validating that the data was inserted */
SELECT * FROM humanresources_dw.dim_employees;

/* Populate dim_jobs */
INSERT INTO `humanresources_dw`.`dim_jobs`
(`job_key`,
`job_title`,
`min_salary`,
`max_salary`)
SELECT `jobs`.`job_id`,
    `jobs`.`job_title`,
    `jobs`.`min_salary`,
    `jobs`.`max_salary`
FROM `humanresources`.`jobs`;

/* Validating that the data was inserted */
SELECT * FROM humanresources_dw.dim_jobs;

/* Populate dim_departments */
INSERT INTO `humanresources_dw`.`dim_departments`
(`department_key`,
`department_name`,
`location_key`)
SELECT `departments`.`department_id`,
    `departments`.`department_name`,
    `departments`.`location_id`
FROM `humanresources`.`departments`;

/* Validating that the data was inserted */
SELECT * FROM humanresources_dw.dim_departments;

/* Populate dim_locations */
INSERT INTO `humanresources_dw`.`dim_locations`
(`location_key`,
`street_address`,
`postal_code`,
`city`,
`state_province`,
`country_key`)
SELECT `locations`.`location_id`,
    `locations`.`street_address`,
    `locations`.`postal_code`,
    `locations`.`city`,
    `locations`.`state_province`,
    `locations`.`country_id`
FROM `humanresources`.`locations`;

/* Validating that the data was inserted */
SELECT * FROM humanresources_dw.dim_locations;

/* Populate fact */
INSERT INTO `humanresources_dw`.`fact`
(`fact_key`,
`employee_key`,
`job_key`,
`department_key`,
`location_key`,
`hire_date`,
`salary`
)
SELECT e.employee_id,
		e.employee_id,
		j.job_id,
        d.department_id,
        l.location_id,
        e.hire_date,
        e.salary
FROM humanresources.jobs AS j
INNER JOIN humanresources.employees AS e
ON j.job_id = e.job_id
INNER JOIN humanresources.departments AS d
ON e.department_id = d.department_id
INNER JOIN humanresources.locations as l
ON d.location_id = l.location_id;

/* Validating that the data was inserted */
SELECT * FROM humanresources_dw.fact;
