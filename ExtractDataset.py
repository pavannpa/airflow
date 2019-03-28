"""Goal: Simple ETL mechanism with communication between operators
	Extract from source database
"""
import logging
import pandas
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.oracle_hook import OracleHook

log = logging.getLogger(__name__)

class ExtractDataset(BaseOperator):
	#template_fields = {'task_id'}

	@apply_defaults
	def __init__(self,srcdb,sql_stmt,*args,**kwargs):
		self.srcdb = srcdb
		self.sql_stmt = sql_stmt
		super(ExtractDataset,self).__init__(*args,**kwargs)


	def execute(self,context):
		"""context : context objects has two sets of information 
						1. Task Instance 
								This contains dag_id,hostname,jobid,key,taskid etc
						2. Task
								This contains dag_id, upstream taskid and downstram taskid etc
								(Note: Even if the design contains to only downstream jobs airflow maps to the parent 
								task which would invoke current operator)
								"""
		#Depending on the source database initiate the corresponding hook
		self.hook = OracleHook(oracle_conn_id=self.srcdb)
		conn = self.hook.conn 
		dataframe = pandas.read_sql(self.sql_stmt,conn)
		tempfilename = "/home/ubuntu/test" + 'temp.csv'
		dataframe.to_csv(tempfilename,sep='|',header=False,index=False,encoding='utf-8')
		conn.close()

		#After persisting the csv file to temp location I am pushing the file location to the transform operator rather 
		#than passing it as a parameter
		task_instance = context['task_instance']
		task_instance.xcom_push(key=context['dag_run'].run_id,value = tempfilename)
		log.info("Extracttion complete ")

class ExtractDatasetPlugin(AirflowPlugin):
	name = "extract_dataset"
	operators = [ExtractDataset]

