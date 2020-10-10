import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('WordCount').getOrCreate()

inputDF = spark.read.text('book.txt')

words = inputDF.select(func.explode(func.split(inputDF.value, '\\W+')).alias('word'))
words.filter(words.word != '')

lowerCaseWords = words.select(func.lower(words.word).alias('word'))

wordCount = lowerCaseWords.groupBy('word').count()

wordCountSorted = wordCount.sort('count')

wordCountSorted.show(wordCountSorted.count())

spark.stop()