import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('MinTemperatures').getOrCreate()

schema = StructType([StructField('stationID', StringType(), True),
                        StructField('date', IntegerType(), True),
                        StructField('measure_type', StringType(), True),
                        StructField('temperature', FloatType(), True)])

df = spark.read.schema(schema).csv('1800.csv')
df.printSchema()


minTemp = df.filter(df.measure_type.contains('TMIN')).select('stationID','temperature')
minTempByStation = minTemp.groupBy('stationID').min('temperature')
minTempByStation.show()

minTempByStationF = minTempByStation.withColumn('temperature', func.round(func.col('min(temperature)') * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                    .select('stationID', 'temperature').sort('temperature')

minTempByStationF.show()
                                                  
results = minTempByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()