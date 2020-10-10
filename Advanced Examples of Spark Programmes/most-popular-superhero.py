import findspark
findspark.init()

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('MostPopularHero').getOrCreate()

def parseLine(line):
    fields = line.split()
    return Row(heroID=int(fields[0]), numHeroes = len(fields)-1)

def getHeroNames():
    heroNames = {}
    with open('Marvel+Names','r', errors='ignore') as f:
        for line in f:
            fields = line.split()
            heroNames[int(fields[0])] = fields[1]
    return heroNames

namesDict = spark.sparkContext.broadcast(getHeroNames())

def nameMapper(heroID):
    return namesDict.value[heroID]

nameMapperUDF = func.udf(nameMapper)

rdd = spark.sparkContext.textFile('Marvel+Graph')
rdd = rdd.map(parseLine)
heroDF = rdd.toDF().groupBy('heroID').sum('numHeroes')
heroWithNames = heroDF.withColumn('heroName', nameMapperUDF(func.col('heroID')))
heroWithNamesSorted = heroWithNames.select('heroName', 'sum(numHeroes)').sort(func.desc('sum(numHeroes)'))

heroWithNamesSorted.show()

spark.stop()