# airflow
Airflow repository contains demo codes to setup ETL
ETLDAG - Demo Dag with extract , transform and load steps

Extract Dataset : 
  1. Contains connection to source database
  2. Extract data into csv using pandas
  3. Writes the csv to temporary location
  4. Using xcom_push I push the temporary file location to transform step
  
Transform Dataset:
  1.Using xcom_pull I extract the temporary file location
  2. Using boto3 library load it to a aws s3 bucket
  3. Using xcom_push I push the s3 file location to load step
 
Load Dataset:
  1.Using xcom_pull I extract the s3 file location
  2. Connect to redshift using postgres hook
  3. copy the csv to redshift
  4. delete the temporary file
  
 

