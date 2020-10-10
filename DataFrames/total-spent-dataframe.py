import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName('TotalAmountSpent').getOrCreate()

schema = StructType([
    StructField('custID', IntegerType(), True),
    StructField('itemID', IntegerType(), True),
    StructField('amount', FloatType(), True)
])

df = spark.read.schema(schema).csv('customer-orders.csv')
amounts = df.select('custID', 'amount')
amountsByCustID = amounts.groupBy('custID').agg(func.round(func.sum('amount'),2).alias('total_amount'))
amountsByCustIDSorted = amountsByCustID.sort(func.desc('total_amount'))

amountsByCustIDSorted.show(amountsByCustIDSorted.count())
spark.stop()