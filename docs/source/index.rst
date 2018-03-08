===================
Meerkat Abacus
===================

The purpose of the abacus component of Meerkat is to set up the database, import all data and then translate raw data into defined variables in the data table.

Abacus uses a PostgreSQL database and all functionality is run in celery tasks. From this module all of these tasks are orchestrate and then run by the celery worker defined in meerkat_runner. When started meerkat abacus sets up all the required database strcture and then process all the existing data. Regular running tasks then update the database with new data as it arrives. 

Abacus is easily configurable to run for different implementations (or countries). The country specific configuration is detailed below.

------------------
Structure
------------------
config.py: application configuration

model.py: specifies the database model

data_management.py: implements all the database functionality

tasks.py: implements task queue for adding new data and aggregating new data

util: various utility functions used 

codes: scripts for turning data into codes

country_config: configuration files for a "null island"

test: unittests for meerkat_abacus


----------------
Data Flow
----------------

The best way to understand meerkat abacus is to consider the data flow. The aim of the meerkat abacus is to take raw data, combine it with metadata like location information and produce structured data that is easy to use for meerkat_api. The main abstraction in this process is the use of codes to translate raw data into configurable codes.

1. Create database and setup all tables according to the model.py file.
2. Import location data.
   We import zones, regions, districts and clinics from location files specified in the config directory. 
3. Import variables.
   We import variables from the codes file. This code file includes all the specfic codes needed for the current implementation.
4. Get existing data or create new *fake* data.
   Abacus supports multiple sources for data as explained in detail below or fake data can be generated as specified in the configuration files. 
5. Import data in to db.
   We import all the data  into the db, making sure only data from approved tablets are imported. We store all the form data by rows in a JSONB database column. This means that we make no assumptions about what fields exists or how the data is structured. We can also define quality controls on the data to remove wrong data. These quality controls are implemented in the codes file. 
6. We translate raw data into structured data. The structured data has on out of three types. Case, Visit or Register. A Case is record that spans the whole history of a patient for one disease. So a patient could have two cases if the system registered them with two different diseases. A visit is one visit to the clinic, either a new visit or a return visit. A register is for aggregated data entry, mainly used for the daily registers.
7. We go through each data type in the data_types.csv file to determine which rows in the raw data correspond to this data type. We then find all the linked rows to this data row.
8.  The variables we have imported tells us how to translate raw data from the forms into structured data. E.g the variables for gender tells us what field in the raw_data should be treated as gender. After this translation one should not need to know any details about the structure of the raw data. This step is the most time-consuming and some effort has been spent to optimise the speed. Indivdual alerts are added at this stage.
9. We determine threshold based alerts. 
10. The data is ready for use by the API and frontend. 



---------------
Variables
---------------

The main abstraction in meerkat_abacus is that we translate raw data from forms by specifically defined codes or variables. After the raw data from the form has been mapped to the correct variables one should not need to know anything about the form structure any more.

The codes are specified in a codes.csv file, e.g country_config/demo_codes.csv. Abacus currently supports the following codes:

* match: The value has to match exactly any of the conditions
* sub_match: A substring of the value has to match any of the conditions
* not_null: Tests that the column is not empty
* between: We calculate a value that has to be between the two condtions.
* calc: We calcaulate a value and return that
* value: We return the value

This basic types can be compined with "and" and "or". 

After the transformation to raw data the structured data will be stored in the *data* table.

Each code has an id and a category in addition to the information that defines the code. The id is then used to query the data later. For example if we have a raw data form where gender is stored in a column called patient./gender we would have for example a code called "female" of the type match that checks if the patient./gender column equals *female*. If it does the corresponding record in the data table will include the id *female* in the *variables* column. In the API it is the easy to determine the number of females by asking for the all the recrods where the *variables* column includes *female*.

In addition to the id, the category is also important. Continuing with the example above the variable would also have a category with name *gender*. After the processing we would have a key-value pair in the *categories* column of *gender:female*. As patients can also be male we will have other rows with *gender:male*. This makes it easy to group by a category. 


--------------------
Configuration
--------------------
Abacus is very configurable. Everything from data sources to fake data generation cane be configured. In the meerkat_abacus repository there is a set of configuration files for *demo*. These can be used to setup the demo system and as templates for creating configurations for other implementations. 

The config.py file has the application level configuration and also imports the country specific configs. Many of the application level configuration variables can be overwritten by environmental variables:

* MEERKAT_ABACUS_DB_URL: db_url
* DATA_DIRECTORY: path to directory where we store data csv files
* COUNTRY_CONFIG_DIR: path to directory with country config
* COUNTRY_CONFIG: name of country config file
* HERMES_API_ROOT: url to hermes instalation for sending of alerts and emails
* MEERKAT_TEST_EMAILS: Shall test emails be sent on startup
* INITIAL_DATA_SOURCE: Where initial data should be pulled. The available options are:
  AWS_S3: Pull data in csv files from AWS S3. Bucket information provided in the country configs.
  LOCAL_CSV: Use local csv files in meerkat_abacus/data folder
  AWS_RDS: Reads data from an AWS database
  LOCAL_RDS: Reads data from a local database
* STREAM_DATA_SOURCE: Where to stream data from. Options are:
  AWS_S3: Periodically download new S3 files to update data
  AWS_SQS: Read data from a specified sqs queue
* SQS_ENPOINT: endpoint to read sqs data from
  

The country level configuration needs the following information:

Main config file: 
--------------------
s3_bucket: the url to the s3 bucket if one is used

country_config dictionary: this dictionary includes almost all the information about the country such as:

* name: name of country
* tables: name of the forms/db tables we are using
* codes_file: name of codes file
* links_file: name of file with link defs
* country_tests: name of files that implements some country specific testing
* epi_week: how epi_weeks are calculated, international gives the start of epi week 1 at 01/01. day:week_day gives the start on the first week_day(Mon=0) after 01/01
* locations: specifies the csv files with location data for the region, district and clinic level
* form_dates: which field in the form gives the date of the form
* fake_data:  how to generate fake data for the form
* alert_data: what data from the case reports to include in alerts
* alert_id_length: the number of characters from the uuid to take as the alert id

  
Locations
-----------

We have potentially four levels of locations: Zones, Regions, Districts and Clinics.

Each level needs a different csv file with locations. For clinics, each record is one tablet with a specific deviceid. Tablets with the same clinic name in the same district will be merged into one clinic. Each clinic provides the gps coordinates. We can also specify geojson files that have the polygon information for the areas of the other locations.

Codes
------
A codes file is needed to specify how to translate the raw data into useful data. See variables for details on naming conventions
 

----------------------------
Documentation
----------------------------

.. toctree::
   :maxdepth: 4

   meerkat_abacus


------------------
Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
