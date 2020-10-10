import findspark
findspark.init()

method = 2 # Method can be 1 or 2
import re
from pyspark import SparkConf, SparkContext

def normalizewords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf = conf)

book = sc.textFile('Book')
words = book.flatMap(normalizewords)
wordCount = words.countByValue() if method==1 else words.map(lambda x : (x,1)).reduceByKey(lambda x , y : x + y).collect()

for word, count in sorted(wordCount.items() if method==1 else wordCount, key = lambda x : x[1]):
    cleanWord = word.encode('ascii','ignore')
    if (cleanWord):
        print(cleanWord, count)