"""Goal: Simple ETL mechanism with communication between operators
	Load : To Redshift
"""
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
log = logging.getLogger(__name__)

class LoadDataset(BaseOperator):
	#template_fields = {'task_id'}
	@apply_defaults
	def __init__(self,s3_bucket,targetdb,targettbl,*args,**kwargs):
		self.s3_bucket = s3_bucket
		self.targetdb = targetdb
		self.targettbl = targettbl
		super(LoadDataset,self).__init__(*args,**kwargs)

	def execute(self,context):
		self.hook = PostgresHook(postgres_conn_id=self.targetdb)
		conn = self.hook.conn 
		task_instance = context['task_instance']
		s3Key = task_instance.xcom_pull(context['task'].upstream_list[0].task_id,key=context['dag_run'].run_id)
		if s3Key is not None:
			log.info('S3Key ' + s3key)
			load_stmt = """copy {0} from 's3://{1}/{2}' iam_role 'arn:aws:iam::123456789012:role/MyRedshiftRole' \
							delimiter '{3}' acceptinvchars '.' fillrecord emptyasnull blankasnull trimblanks truncatecolumns \
							dateformat 'auto' region 'us-east-1' 'csv' """.format(self.targettbl,self.s3_bucket,s3Key)
			conn.execute(load_stmt)
			conn.commit()
			conn.close()
		else:
			raise Exception('File not found at Load operation')


class LoadDatasetPlugin(AirflowPlugin):
	name = "load_dataset"
	operators = [LoadDataset]
		