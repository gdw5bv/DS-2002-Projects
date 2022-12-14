# Databricks notebook source
# MAGIC %md
# MAGIC # DS2002 Capstone Project 2

# COMMAND ----------

pip install pymongo

# COMMAND ----------

# Prerequisites
# Importing required libraries
import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# Instantiating global variables

# Azure SQL Server Connection Information #####################
jdbc_hostname = "ds2002-mysql-gdw5bv.mysql.database.azure.com"
jdbc_port = 3306
src_database = "humanresources"

connection_properties = {
  "user" : "user",
  "password" : "Passw0rd",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "sandbox"
atlas_database_name = "humanresources"
atlas_user_name = "gdw5bv"
atlas_password = "Passw0rd"

# Data Files (JSON) Information ###############################
dst_database = "humanresources_dw4"

base_dir = "dbfs:/FileStore/ds2002-capstone"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_employees/bronze"
output_silver = f"{database_dir}/fact_employees/silver"
output_gold   = f"{database_dir}/fact_employees/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_employees", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)

# COMMAND ----------

# Defining global functions

# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl =  f"jdbc:mysql://{host_name}:{port}/{db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
# ######################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.dkkrh.mongodb.net/{db_name}?retryWrites=true&w=majority"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.zibbf.mongodb.net/{db_name}?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# Populating dimensions by ingesting reference (Cold-path) data
# Fetching reference data from an Azure SQL Database
# Creating a new databricks metadata database, and then create a new table that sources its data from a view in an Azure SQL database

# COMMAND ----------

# Dropping database if exists so that when rerunning the create database, it doesn't cause error of already having that database

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS humanresources_dw4 CASCADE;

# COMMAND ----------

# Creating database if not exist so that when rerunning, it doesn't cause error of already having that database if not dropped already

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS humanresources_dw4
# MAGIC COMMENT "DS 2002 Capstone Project Database"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/humanresources_dw4"
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Capstone Project");

# COMMAND ----------

# Creating view_jobs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_jobs
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds2002-mysql-gdw5bv.database.windows.net:3306;database=humanresources",
# MAGIC   dbtable "Max_Salary.DimJobs",
# MAGIC   user "user",
# MAGIC   password "Passw0rd"
# MAGIC )

# COMMAND ----------

# Creating the the table if not exists

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE humanresources_dw4;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS humanresources_dw4.jobs
# MAGIC COMMENT "Jobs Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/humanresources_dw4/dim_jobs"
# MAGIC AS SELECT * FROM view_jobs
# MAGIC      

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED humanresources_dw4.dim_jobs;

# COMMAND ----------

# Creating a new table that sources its data from a table in an Azure SQL database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_date
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds2002-mysql-gdw5bv.database.windows.net:3306;database=humanresources",
# MAGIC   dbtable "dbo.DimDate",
# MAGIC   user "user",
# MAGIC   password "Passw0rd"
# MAGIC )

# COMMAND ----------

# Dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE humanresources_dw4;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS humanresources_dw4.dim_date
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/humanresources_dw4/dim_date"
# MAGIC AS SELECT * FROM view_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works_dw4.dim_date LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED humanresources_dw4.dim_date;

# COMMAND ----------

# Fetching reference data from a MongoDB Atlas Database
# Viewing the data files on the Databricks File System (DBFS)

# COMMAND ----------

display(dbutils.fs.ls(batch_dir))

# COMMAND ----------

# Creating a new MongoDB Database, and loading JSON data into a new MongoDB collection

# COMMAND ----------

source_dir = '/dbfs/FileStore/ds2002-capstone/source_data/batch'
json_files = {"departments" : 'humanresources_DimDepartments.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# Fetching data from the new MongoDB collection

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC 
# MAGIC val df_departments = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "humanresources_dw4").option("collection", "departments").load()
# MAGIC display(df_departments)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_departments.printSchema()

# COMMAND ----------

# Using the spark dataFrame to create a new table in the databricks (Human Resources) metadata database

# COMMAND ----------

# MAGIC %scala
# MAGIC df_departments.write.format("delta").mode("overwrite").saveAsTable("humanresources_dw4.dim_departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED humanresources_dw4.dim_departments

# COMMAND ----------

# Querying the new table in the databricks metadata database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM humanresources_dw4.dim_departments LIMIT 5

# COMMAND ----------

# Fetching data from a file system
# Using PySpark to read from a CSV file

# COMMAND ----------

locations_csv = f"{batch_dir}/humanresources_DimLocations.csv"

df_locations = spark.read.format('csv').options(header='true', inferSchema='true').load(locations_csv)
display(df_locations)

# COMMAND ----------

df_locations.printSchema()

# COMMAND ----------

df_locations.write.format("delta").mode("overwrite").saveAsTable("humanresources_dw4.dim_locations")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED humanresources_dw4.dim_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM humanresources_dw4.dim_locations LIMIT 5;

# COMMAND ----------

# Verifying dimension tables by showing the tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE humanresources_dw4;
# MAGIC SHOW TABLES

# COMMAND ----------

# Integrating reference data with real-time data
# Using AutoLoader to process streaming (Hot Path) data

# COMMAND ----------

# Bronze Table: Process 'Raw' JSON Data (First step of the Databricks Bronze Silver Gold Architecture)

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaHints", "employee_id INT")
 .option("cloudFiles.schemaHints", "first_name VARCHAR")
 .option("cloudFiles.schemaHints", "last_name VARCHAR")
 .option("cloudFiles.schemaHints", "email VARCHAR")
 .option("cloudFiles.schemaHints", "phone_number VARCHAR")
 .option("cloudFiles.schemaHints", "hire_date DATE")
 .option("cloudFiles.schemaHints", "job_id INT")
 .option("cloudFiles.schemaHints", "salary DECIMAL")
 .option("cloudFiles.schemaHints", "manager_id INT")
 .option("cloudFiles.schemaHints", "department_id INT")
 .option("cloudFiles.schemaLocation", output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(stream_dir)
 .createOrReplaceTempView("employees_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW employees_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() hire_date, input_file_name() source_file
# MAGIC   FROM employees_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees_bronze_tempview

# COMMAND ----------

(spark.table("employees_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_employees_bronze"))

# COMMAND ----------

# Silver Table: Including reference data (Second step of the Databricks Bronze Silver Gold Architecture)

# COMMAND ----------

(spark.readStream
  .table("fact_employees_bronze")
  .createOrReplaceTempView("employees_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employees_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_employees_silver_tempview AS (
# MAGIC   SELECT e.employee_id,
# MAGIC 		 e.employee_id,
# MAGIC 		 j.job_id,
# MAGIC          d.department_id,
# MAGIC          l.location_id,
# MAGIC          e.hire_date,
# MAGIC          e.salary
# MAGIC   FROM humanresources.jobs AS j
# MAGIC   INNER JOIN humanresources.employees AS e
# MAGIC   ON j.job_id = e.job_id
# MAGIC   INNER JOIN humanresources.departments AS d
# MAGIC   ON e.department_id = d.department_id
# MAGIC   INNER JOIN humanresources.locations as l
# MAGIC   ON d.location_id = l.location_id;

# COMMAND ----------

(spark.table("fact_employees_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_employees_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_employees_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED humanresources_dw4.fact_employees_silver

# COMMAND ----------

# Gold Table: Performing aggregation - averaging the salaries of each department and looking at them in descending order to see which department has the highest average salary (Last step of the Databricks Bronze Silver Gold Architecture)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT employees.`department_name` AS `department_name`,
# MAGIC         AVG(employees.`salary`) AS `average_salary_in_department`
# MAGIC FROM `{0}`.`fact_employees_silver` AS employees
# MAGIC INNER JOIN `{0}`.`dim_departments` AS departments
# MAGIC ON employees.department_key = departments.department_key
# MAGIC GROUP BY employees.`department_name`
# MAGIC ORDER BY average_salary_in_department DESC;
