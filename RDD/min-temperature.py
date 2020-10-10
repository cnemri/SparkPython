import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1*(9.0/5.0)+32.0
    return (stationID, entryType, temperature)

lines = sc.textFile('1800.csv')
rdd = lines.map(parseLine)

minTemp = rdd.filter(lambda x : 'TMIN' in x[1])

stationTemps = minTemp.map(lambda x : (x[0], x[2]))

minTemps = stationTemps.reduceByKey(lambda x,y : min(x,y))

results = minTemps.collect()

for result in results:
    print(result[0], f'\t {result[1]:.2f}')



