/* Create the datawarehouse - humanresources_dw database */

CREATE DATABASE `humanresources_dw` /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE humanresources_dw;

CREATE TABLE `dim_employees` (
  `employee_key` int NOT NULL AUTO_INCREMENT,
  `first_name` varchar(20) DEFAULT NULL,
  `last_name` varchar(25) NOT NULL,
  `email` varchar(100) NOT NULL,
  `phone_number` varchar(20) DEFAULT NULL,
  `hire_date` date NOT NULL,
  `job_key` int NOT NULL,
  `salary` decimal(8,2) NOT NULL,
  `manager_key` int DEFAULT NULL,
  `department_key` int DEFAULT NULL,
  PRIMARY KEY (`employee_key`),
  KEY `job_key` (`job_key`),
  KEY `department_key` (`department_key`),
  KEY `manager_key` (`manager_key`)
) ENGINE=InnoDB AUTO_INCREMENT=207 DEFAULT CHARSET=latin1;

CREATE TABLE `dim_jobs` (
  `job_key` int NOT NULL AUTO_INCREMENT,
  `job_title` varchar(35) NOT NULL,
  `min_salary` decimal(8,2) DEFAULT NULL,
  `max_salary` decimal(8,2) DEFAULT NULL,
  PRIMARY KEY (`job_key`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=latin1;

CREATE TABLE `dim_departments` (
  `department_key` int NOT NULL AUTO_INCREMENT,
  `department_name` varchar(30) NOT NULL,
  `location_key` int DEFAULT NULL,
  PRIMARY KEY (`department_key`),
  KEY `location_key` (`location_key`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=latin1;

CREATE TABLE `dim_locations` (
  `location_key` int NOT NULL AUTO_INCREMENT,
  `street_address` varchar(40) DEFAULT NULL,
  `postal_code` varchar(12) DEFAULT NULL,
  `city` varchar(30) NOT NULL,
  `state_province` varchar(25) DEFAULT NULL,
  `country_key` char(2) NOT NULL,
  PRIMARY KEY (`location_key`),
  KEY `country_key` (`country_key`)
) ENGINE=InnoDB AUTO_INCREMENT=2701 DEFAULT CHARSET=latin1;

CREATE TABLE `fact` (
	`fact_key` int NOT NULL AUTO_INCREMENT,
	`employee_key` int DEFAULT NULL,
	`job_key` int NOT NULL,
	`department_key` int DEFAULT NULL,
    `location_key` int DEFAULT NULL,
    `hire_date` date NOT NULL,
    `salary` decimal(8,2) NOT NULL,
    PRIMARY KEY (`fact_key`),
	KEY `employee_key` (`employee_key`),
    KEY `job_key` (`job_key`),
    KEY `department_key` (`department_key`),
    KEY `location_key` (`location_key`)
) ENGINE=InnoDB AUTO_INCREMENT=82 DEFAULT CHARSET=latin1;