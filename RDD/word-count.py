import findspark
findspark.init()

method = 2 # Method can be 1 or 2

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf = conf)

book = sc.textFile('Book')
words = book.flatMap(lambda x : x.split())
wordCount = words.countByValue() if method==1 else words.map(lambda x : (x,1)).reduceByKey(lambda x , y : x + y).collect()

for word, count in sorted(wordCount.items() if method==1 else wordCount, key = lambda x : x[1]):
    cleanWord = word.encode('ascii','ignore')
    if (cleanWord):
        print(cleanWord, count)