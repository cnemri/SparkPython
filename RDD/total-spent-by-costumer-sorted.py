import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('TotalAmountSpent')
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile('customer-orders.csv')
orderAmounts = lines.map(parseLine).reduceByKey(lambda x , y :x + y)
sortedOrderAmounts = orderAmounts.map(lambda x : (x[1], x[0])).sortByKey()

results = sortedOrderAmounts.collect()

for amount, customer in results:
    print('CustomerID : '+str(customer), '\t', f'{amount:.2f} USD')