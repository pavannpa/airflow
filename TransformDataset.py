"""Goal: Simple ETL mechanism with communication between operators
	Transform to S3
"""
import logging
import boto3
import os
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
log = logging.getLogger(__name__)

class TransformDataset(BaseOperator):
	#template_fields = {'task_id'}
	@apply_defaults
	def __init__(self,s3_bucket,s3_folder,*args,**kwargs):
		self.s3_bucket = s3_bucket
		self.s3_folder = s3_folder
		super(TransformDataset,self).__init__(*args,**kwargs)

	def execute(self,context):
		"""Goal :
				Pull the file name
				Push to S3 
		"""
		curr_task_instance = context['task_instance']
		#Extracting parent task id information
		parent_task_instance_id = context['task'].upstream_list[0].task_id 

		#In Extract layer the filename is pushed using run_id,key and task_id as key parameters
		filename = curr_task_instance.xcom_pull(parent_task_instance_id,key=context['dag_run'].run_id)
		if filename is not None:
			s3 = boto3.resource('s3')
			s3key = self.s3_folder + '/' + filename
			s3.Bucket(self.s3_bucket).upload_file(filename,s3key)
			log.info('Loaded file '+filename)
			#For Data privacy deleting the temporary file created by extract task
			os.remove(filename)
			curr_task_instance.xcom_push(key=context['dag_run'].run_id,value=s3key)
		else:
			raise Exception('File not found at Transform Operation')

class TransformDatasetPlugin(AirflowPlugin):
	name = "transform_dataset"
	operators = [TransformDataset]


