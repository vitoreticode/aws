import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from boto3 import client
from pyspark.sql.functions import lit

# ENVIRONMENT VARIABLES
#args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASES', 'S3_RAW_PARQUET_PATH'])

# Loading the contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#job.init(args['JOB_NAME'], args)
job.init("billing_wiseOCS_accounts_csv_to_parquet")

# Where the script will write the files to
#s3_destiny = args['S3_RAW_PARQUET_PATH']

# Sources
# list_databases = args['DATABASES'].split(',')
list_databases = ["billing_wiseocs"]

# Glue object to list the tables within each database
client = client('glue', region_name='us-east-2')

for database in list_databases:
    response = client.get_tables(DatabaseName=database.lower())
    # Transforms the database name to be used as a filepath
    database_source = database.replace('_raw_csv', '')

    for table in response['TableList']:
        try:
            #s3_destiny = args['S3_RAW_PARQUET_PATH']
            s3_destiny = "s3://lucas-vitoreti/parquet/"
            # Stores the date and time that the job ran for that particular table
            processed_date = datetime.now().strftime('%Y-%m-%d %H')
            print('Table catalog: {0}. s3 origin: {1}'.format(table['Name'], table['StorageDescriptor']['Location']))
            table_name = table['Name']
            s3_destiny = '{root_folder}source={database}/table={table_folder}'.format(
                root_folder=s3_destiny,
                database=database_source,
                table_folder=table_name
            )
            
            # Loads the csv table
            datasource = glueContext.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table_name,
                transformation_ctx=table_name
            )
            df = datasource.toDF()

            dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
            
            # And saves the new table as a parquet file
            final_df = glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": s3_destiny,
                },
                format="parquet",
                transformation_ctx=table_name
            )
            
            # Commits the job to save the bookmark info for each table
            job.commit()
        except Exception as exc:
            print('Error: {}'.format(exc))
