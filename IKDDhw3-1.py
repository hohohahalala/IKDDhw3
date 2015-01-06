from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("IKDDhw3").setMaster("local")
sc = SparkContext(conf=conf)
file = sc.textFile("/Users/hohohahalala/Documents/IKDD/IKDDhw3/5000.txt.utf-8.txt")

counts = file.flatMap(lambda line: line.split(" ")) \
             .collect()
print len(counts)
