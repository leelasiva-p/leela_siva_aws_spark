import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df=spark.read.format("csv").option("header",True).option("path","s3://crawler-testing-sep2/orders_data_1000.csv").load()

df.write.format("csv").option("header",True).option("path","s3://crawler-testing-sep2/sept5").save()


# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1757130566510 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://crawler-testing-sep2/orders_data_1000.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1757130566510")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AmazonS3_node1757130566510, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757130545523", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1757130738783 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1757130566510, connection_type="s3", format="glueparquet", connection_options={"path": "s3://crawler-testing-sep2", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1757130738783")

job.commit()