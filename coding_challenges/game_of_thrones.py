import re
from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__ == '__main__':
    sc = SparkContext()
    sql = SparkSession(sc)
    # folder = './data/mini_reverse'
    folder = './data/indexing'
    whole_text = sc.wholeTextFiles(folder)
    print(whole_text.map(lambda x: (x[0].split('/')[-1], re.findall(r'\w+', x[1])))\
        .flatMapValues(lambda x: x)\
        .map(lambda x: (x[1],list(x[0])))\
        .reduceByKey(lambda x, y: x+y)\
        .collect())
