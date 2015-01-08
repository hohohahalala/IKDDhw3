from pyspark import SparkConf, SparkContext
import sys,os

def main():
	conf = SparkConf().setAppName("IKDDhw3").setMaster("local")
	sc = SparkContext(conf=conf)
	try:
		file_name = sys.argv[1]
		file = sc.textFile(file_name)
		counts = file.flatMap(lambda line: line.split()) \
				.collect()
		print "----------------------------------------------------------"
		print "Total word number: " + str(len(counts))
	except:
		print "Usage: \"./bin/pyspark python_file_path data_file_path\""
		exit()

if __name__ == '__main__':
	main()