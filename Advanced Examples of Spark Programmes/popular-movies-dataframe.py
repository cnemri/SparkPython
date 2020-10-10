import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql import functions as func

import codecs

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

def loadMovieNames():
    movieNames = {}
    with codecs.open('ml-100k/u.item','r',encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames

nameDict = spark.sparkContext.broadcast(loadMovieNames())


schema = StructType([
    StructField('userID', IntegerType(), nullable=True),
    StructField('movieID', IntegerType(), nullable=True),
    StructField('rating', IntegerType(), nullable = True),
    StructField('epochs_second', LongType(), nullable = True)
])

ratingsDF = spark.read.schema(schema).csv('ml-100k/u.data', sep="\t")

movieCounts = ratingsDF.groupBy('movieID').count()

def lookUpName(movieID):
    return nameDict.value[movieID]

lookUpNameUDF = func.udf(lookUpName)

moviesWithNames = movieCounts.withColumn('movieTitle', lookUpNameUDF(func.col('movieID')))

sortedMoviesWithNames = moviesWithNames.select('movieTitle', 'count').orderBy(func.desc('count'))

sortedMoviesWithNames.show(10, False)

spark.stop()