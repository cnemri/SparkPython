import findspark
findspark.init()

from pyspark.sql import SparkSession, Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1]).encode('utf-8'),\
        age = int(fields[2]), numFriends = int(fields[3]))

lines = spark.sparkContext.textFile('fakefriends.csv')
people = lines.map(mapper)

# Infer the schema and register the dataframe as a table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

# SQL can be run over DataFrames that have been registered as a table
teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')

for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy('age').count().orderBy('age').show()

spark.stop()
