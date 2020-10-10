import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName('AvgFriendsByAge').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema','true')\
    .csv('fakefriends-header.csv')

# uncomment to inspect schema
# people.printSchema()

friendsByAge = people.select('age', 'friends')

# method 1
friendsByAge.groupBy('age').avg('friends').show()

# sorted
friendsByAge.groupBy('age').avg('friends').sort('age').show()

# formatted more nicely
friendsByAge.groupBy('age').agg(func.round(func.avg('friends'),2)).sort('age').show()

# with a custom column name
friendsByAge.groupBy('age').agg(func.round(func.avg('friends'),2).alias('friends_avg')).sort('age').show()

spark.stop()