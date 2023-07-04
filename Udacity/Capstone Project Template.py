# Databricks notebook source
# MAGIC %md
# MAGIC # Project Title
# MAGIC ### Data Engineering Capstone Project
# MAGIC
# MAGIC #### Project Summary
# MAGIC This project was made using Spark in order to make a datawarehouse in parquet file format that reflects inmigration data in US airports. It's used a star schema with a facts table an dimensional tables.
# MAGIC
# MAGIC The project follows the follow steps:
# MAGIC * Step 1: Scope the Project and Gather Data
# MAGIC * Step 2: Explore and Assess the Data
# MAGIC * Step 3: Define the Data Model
# MAGIC * Step 4: Run ETL to Model the Data
# MAGIC * Step 5: Complete Project Write Up

# COMMAND ----------

from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *

from sources import Source
from cleaner import Cleaner
from transformer import Transformer
from modelizer import Modelizer
from validator import Validator

# COMMAND ----------

#not needed because it did run in databricks itself. 
#Build spark session
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()

# COMMAND ----------

# MAGIC %fs ls FileStore/Udacity/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sources Configurations

# COMMAND ----------

paths = {
    "demographics" : "dbfs:/FileStore/Udacity/us_cities_demographics.csv",
    "airports" :  "dbfs:/FileStore/Udacity/airport_codes_csv.csv",
    "sas_data" : "dbfs:/FileStore/Udacity/sas_data",
    "us_states" : "dbfs:/FileStore/Udacity/us_states.csv",
    "cities" : "dbfs:/FileStore/Udacity/cities.csv",
    "countries" : "dbfs:/FileStore/Udacity/countries.csv",
    "visa" : "dbfs:/FileStore/Udacity/visa.csv",
    "inmigrant_airports" : "dbfs:/FileStore/Udacity/airports.csv",
    "mode" : "dbfs:/FileStore/Udacity/mode.csv",
    "airlines" : "dbfs:/FileStore/Udacity/airlines.dat"
}

# COMMAND ----------

# MAGIC %fs ls FileStore/Udacity

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Scope the Project and Gather Data
# MAGIC
# MAGIC ### **Scope**
# MAGIC
# MAGIC This project will pull data from all sources and create fact and dimension tables to show movement of immigration in US.
# MAGIC
# MAGIC ### **Describe and Gather Data**
# MAGIC
# MAGIC **U.S. City Demographic Data (demog)**: comes from OpenSoft and includes data by city, state, age, population, veteran status and race.
# MAGIC
# MAGIC **I94 Immigration Data (sas_data)**: comes from the US National Tourism and Trade Office and includes details on incoming immigrants and their ports of entry.
# MAGIC
# MAGIC **Airport Code Table (airport)**: comes from datahub.io and includes airport codes and corresponding cities.
# MAGIC
# MAGIC **Countries (countries)**: comes from I94_SAS_Labels_Descriptions.SAS 
# MAGIC
# MAGIC **Visas (visa)**: comes from I94_SAS_Labels_Descriptions.SAS 
# MAGIC
# MAGIC **Inmigrant Entry Mode (mode)**: comes from I94_SAS_Labels_Descriptions.SAS 
# MAGIC
# MAGIC **Airlines**: comes from https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all the sources

# COMMAND ----------

source = Source(spark, paths)

demog = source.get_cities_demographics_raw()
airport=source.get_airports_raw()
sas_data = source.get_inmigration_raw()
countries = spark.read.format("csv").option("header", "true").option("delimiter", ',').load("dbfs:/FileStore/Udacity/countries.csv")
visa = source.get_visa_raw()
mode = source.get_mode_raw()
airlines = source.get_airlines()

# COMMAND ----------

# MAGIC %md
# MAGIC #### View Sources Datasets in raw format

# COMMAND ----------

demog.show()

# COMMAND ----------

airport.show()

# COMMAND ----------

sas_data.show()

# COMMAND ----------

countries.show()

# COMMAND ----------

visa.show()

# COMMAND ----------

mode.show()

# COMMAND ----------

airlines.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Explore and Assess the Data
# MAGIC All the clean steps are doing in cleaner.py file an its explanations.
# MAGIC
# MAGIC ### **Cleaning Steps**
# MAGIC
# MAGIC Main steps are:
# MAGIC
# MAGIC  * Clean demographics dataset, filling null values withn 0 and grouping by city and state and pivot Race in diferent columns
# MAGIC  * Clean airports dataset filtering only US airports and discarting anything else that is not an airport. Extract iso regions and cast as float elevation feet.
# MAGIC  * Clean the inmigrantion dataset. Rename columns with understandable names. Put correct formats in dates and select only important columns
# MAGIC  * Clean airlines dataset and filter only airlines with IATA code.

# COMMAND ----------

demog_clean = Cleaner.get_cities_demographics(demog)
demog_clean.show()

# COMMAND ----------

airport_clean = Cleaner.get_airports(airport)
airport_clean.show()

# COMMAND ----------

inmigrant_clean = Cleaner.get_inmigration(sas_data)
inmigrant_clean.show()

# COMMAND ----------

countries_clean = Cleaner.get_countries(countries)
countries_clean.show()

# COMMAND ----------

visa_clean = Cleaner.get_visa(visa)
visa_clean.show()

# COMMAND ----------

mode_clean = Cleaner.get_mode(mode)
mode_clean.show()

# COMMAND ----------

airlines_clean = Cleaner.get_airlines(airlines)
airlines_clean.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Define the Data Model
# MAGIC #### 3.1 Conceptual Data Model
# MAGIC Map out the conceptual data model and explain why you chose that model
# MAGIC
# MAGIC #### Star Schema
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC
# MAGIC * dim_demographics
# MAGIC     * State, **state_code**, Total_Population, Male_Population, Female_Population, American_Indian_and_Alaska_Native, Asian, Black_or_African-American, Hispanic_or_Latino, White, Male_Population_Ratio, Female_Population_Ratio, American_Indian_and_Alaska_Native_Ratio, Asian_Ratio, Black_or_African-American_Ratio, Hispanic_or_Latino_Ratio, White_Ratio. 
# MAGIC * dim_airports
# MAGIC     * ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, **local_code**, coordinates.
# MAGIC * dim_airlines
# MAGIC     * Airline_ID, Name, **IATA**, ICAO, Callsign, Country, Active.
# MAGIC * dim_countries:
# MAGIC     * **cod_country**, country_name
# MAGIC * dim_get_visa:
# MAGIC     * **cod_visa**, visa.
# MAGIC * dim_get_mode:
# MAGIC     * **cod_mode**, mode_name.
# MAGIC
# MAGIC **Fact Table:**
# MAGIC
# MAGIC * immigration_fact_table
# MAGIC     * **cic_id**, **cod_port**, **cod_state**, visapost, matflag, dtaddto, gender, **airline**, admnum, fltno, visatype, **cod_visa**, **cod_mode**, **cod_country_origin**, **cod_country_cit**, year, month, bird_year, age, counter, arrival_date, departure_date, arrival_year, arrival_month, arrival_day.
# MAGIC
# MAGIC #### 3.2 Mapping Out Data Pipelines
# MAGIC There are two steps:
# MAGIC  * Tranform data:
# MAGIC      * Transform demographics dataset grouping by state an calculate all the totals and ratios for every race in every state.
# MAGIC      * Transform inmigration dataset on order to get arrival date in different columns (year, month, day) for partitioning the dataset.
# MAGIC   
# MAGIC  * Generate Model (Star Schema):
# MAGIC      * Create all dimensions in parquet.
# MAGIC      * Create fact table in parquet particioned by year, month, day of th arrival date.
# MAGIC      * Insert in fact table only items with dimension keys right. For integrity and consistency.
# MAGIC  

# COMMAND ----------

demog_transformer = Transformer.transform_demographics(demog_clean)
demog_transformer.show()

# COMMAND ----------

inmigrant_transformer = Transformer.transform_inmigrants(inmigrant_clean)
inmigrant_transformer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Run Pipelines to Model the Data 
# MAGIC #### 4.1 Create the data model
# MAGIC Build the data pipelines to create the data model.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration to write the Model

# COMMAND ----------

paths_write = {
    "demographics" : "dbfs:/FileStore/Udacity/model/demographics.parquet",
    "airports" :  "dbfs:/FileStore/Udacity/model/airports.parquet",
    "airlines" : "dbfs:/FileStore/Udacity/model/airlines.parquet",
    "countries" : "dbfs:/FileStore/Udacity/model/countries.parquet",
    "visa" : "dbfs:/FileStore/Udacity/model/visa.parquet",
    "mode" : "dbfs:/FileStore/Udacity/model/mode.parquet",
    "facts" : "dbfs:/FileStore/Udacity/model/facts_inmigration.parquet"
}

# COMMAND ----------

model = Modelizer(spark, paths_write)

# COMMAND ----------

model.modelize(inmigrant_transformer, demog_transformer, airport_clean, airlines_clean, countries_clean, visa_clean, mode_clean)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/Udacity/model

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2 Data Quality Checks
# MAGIC Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
# MAGIC  * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
# MAGIC  * Unit tests for the scripts to ensure they are doing the right thing
# MAGIC  * Source/Count checks to ensure completeness
# MAGIC  
# MAGIC Run Quality Checks

# COMMAND ----------

validator = Validator(spark, paths_write)

# COMMAND ----------

facts = validator.get_facts()

# COMMAND ----------

dim_demographics, dim_airports, dim_airlines, dim_countries, dim_get_visa, dim_get_mode = validator.get_dimensions()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate all dimensions have data

# COMMAND ----------

validator.exists_rows(dim_demographics)

# COMMAND ----------

validator.exists_rows(dim_airports)

# COMMAND ----------

validator.exists_rows(dim_airlines)

# COMMAND ----------

validator.exists_rows(dim_countries)

# COMMAND ----------

validator.exists_rows(dim_get_visa)

# COMMAND ----------

validator.exists_rows(dim_get_mode)

# COMMAND ----------

validator.exists_rows(facts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate integrity and consistency of the model

# COMMAND ----------

validator.check_integrity(facts, dim_demographics, dim_airports, dim_airlines, dim_countries, dim_get_visa, dim_get_mode)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.3 Data dictionary 
# MAGIC Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.
# MAGIC
# MAGIC Create file **DataDictionary.md**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Complete Project Write Up
# MAGIC * Clearly state the rationale for the choice of tools and technologies for the project.
# MAGIC * Propose how often the data should be updated and why.
# MAGIC * Write a description of how you would approach the problem differently under the following scenarios:
# MAGIC  * The data was increased by 100x.
# MAGIC  * The data populates a dashboard that must be updated on a daily basis by 7am every day.
# MAGIC  * The database needed to be accessed by 100+ people.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. For this project I used Apache Spark on Databircks to do all the processing data and create the model. The reason for this is because Spark can scale a lot of data and the library spark.sql has many tools to transform data. The data persisted in parquet files can scale to losts of terabytes without any problems.
# MAGIC
# MAGIC 2. The data should be updated every day. We can use Apache Airflow to ingest every day (arrival date) because fact table are partitioned bay arrival date.
# MAGIC
# MAGIC 3. Under the following scenarios, I would approach the problem differently:
# MAGIC
# MAGIC If the data was increased by 100x, no problem --> Spark can do it.
# MAGIC
# MAGIC To update on a daily basis I would use Apache Airflow to create a schedule to update all the data,
# MAGIC
# MAGIC If the data needs to be accessed by 100+ people, we can use Hive, Spark sql template views, ...

# COMMAND ----------


