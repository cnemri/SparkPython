import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import regexp_extract

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()

logs = spark.readStream.text('logs')


# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = logs.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2 = logsDF.withColumn('eventTime', func.current_timestamp())
topUrls = logsDF2.groupBy(func.window(func.col('eventTime'), '30 seconds', '10 seconds'), func.col('endpoint')).count()


sortedEndPointCounts = topUrls.orderBy(func.col('count').desc())

query = ( sortedEndPointCounts.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
