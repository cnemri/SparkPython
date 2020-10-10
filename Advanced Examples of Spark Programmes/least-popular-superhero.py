import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('LeastPopularHero').getOrCreate()

schema = StructType([
    StructField('heroID', IntegerType(), True),
    StructField('heroName', StringType(), True)
])

heroNamesDF = spark.read.schema(schema).option('sep', ' ').csv('Marvel+Names')

connections = spark.read.text('Marvel+Graph')

heroConnectionsDF = connections.withColumn('heroID', func.split('value', ' ')[0])\
                        .withColumn('connections', func.size(func.split('value', ' '))-1)\
                        .groupBy('heroID').agg(func.sum(func.col('connections')).alias('connections')).sort('connections')

minConnections = heroConnectionsDF.first()[1]

heroWithNames = heroConnectionsDF.filter(func.col('connections')==minConnections).join(heroNamesDF, on='heroID', how='left')

heroWithNames.show()

spark.stop()