# -*- coding: utf-8 -*-
from pyspark.mllib.recommendation import ALS
from numpy import array
from pyspark import SparkConf, SparkContext
import numpy as np
import itertools
import sys

def main():
	#init
	conf = SparkConf().setAppName("IKDDhw3").setMaster("local")
	sc = SparkContext(conf=conf)

	movies_data = ""
	ratings_data = ""
	try:
		movies_data = sc.textFile(sys.argv[1])
		ratings_data = sc.textFile(sys.argv[2])
	except:
		print "Usage: \"./bin/pyspark python_file_path movies.dat_path ratings.dat_path\""
		exit()

	#get movies.dat => RDD
	movies = movies_data.map(lambda line: ([str(x.encode('ascii', 'ignore')) for x in line.split('::')])[0:2]) \
						.collect()
	
	#get movies dictionary
	movie_dict = {}
	for itera in movies:
		movie_dict[itera[0]] = itera[1]
	
	#get ratings.dat => RDD
	ratings = ratings_data.map(lambda line: ([int(x) for x in line.split('::')])[0:3])
	ratings.cache()

	# get candidates
	candidates = ratings.map(lambda p: [int(p[1]),int(p[0])]) \
					.reduceByKey(lambda a, b: "") \
					.map(lambda l: l[0])

	#start training, build model
	rank = 8
	numIterations = 20
	model = ALS.train(ratings, rank, numIterations)

	#make prediction
	predictions = model.predictAll(candidates.map(lambda x: [9, x])).collect()
	recommendations = sorted(predictions, key = lambda x: x[2], reverse = True)[:10]

	print "Movies recommended for you:"
	for i in xrange(len(recommendations)):
		print ("%2d: %s" % (i + 1, movie_dict[str(recommendations[i][1])]))



if __name__ == '__main__':
	main()



