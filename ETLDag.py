import logging
from airflow import DAG
import datetime
from airflow.operators.extract_dataset import ExtractDataset
from airflow.operators.transform_dataset import TransformDataset
from airflow.operators.load_dataset import LoadDataset


sql_stmt = "Select * from public.Employee"


default_args = {
	'owner' : 'panan'
	'provide_context' : True,
	'start_date': datetime.date.now(),
	'email' : ['abcdef@gmail.com'],
	'email_on_failure':True,
	'email_on_retry':True,
	'retry_delay':datetime.timedelta(minutes=3)
}

dag = DAG('pavan_etl_test',
			default_args = default_args,
			schedule_interval = '@daily')

extract = ExtractDataset(
			task_id = 'extract',
			srcdb='oracle_db',
			sql_stmt = sql_stmt
			dag=dag
		)

tranform = TransformDataset(
			task_id = 'transform',
			s3_bucket = 'testextractload',
			s3_folder = 'temp',
			dag=dag
		)

load = LoadDataset(
			task_id = 'load',
			s3_bucket = 'testextractload',
			targetdb = 'redshift_cluster',
			targettbl = 'public.Employee',
			dag=dag
	)

extract.set_downstream(transform)
transform.set_downstream(load)