import sys
from collections import defaultdict
from itertools import combinations
import numpy as np
import random
import csv
import pdb
from math import log10
from pyspark import SparkConf,SparkContext

def parseVector(line):
	'''
	Parse each line of the specified data file, assuming a "\t" delimiter.
	Converts each rating to a float
	'''
	line = line.split("\t")
	return line[0],(line[1],line[2])

def calculateScore(line):
	score = 0.0
	line = line.split('\t')
	artist = line[2]
	genre = line[1]
	if(genre in broadCastedData.value):
		score += 1.0 + log10(broadCastedData.value[str(genre)])
	if(artist in broadCastedData.value):
		score += 1.0 + log10(broadCastedData.value[str(artist)])
	return line[0], score
	
conf = SparkConf()
sc = SparkContext(conf = conf)
#sc = SparkContext(sys.argv[1], "PythonUserCF")
data = sc.textFile("data1.txt")
songsAttributes = sc.textFile("songatt.txt")
userId = "c1"

# [(i1,r1), (i2, r2)...] for logged in user
itemRatings = data.map(parseVector).filter(lambda x: x[0] == userId).map(lambda x : (x[1][0],x[1][1]))
itemRatingsMap = itemRatings.collectAsMap()

#genreArtistScores = songsAttributes.map(parseVector).filter(lambda x: x[0] in itemRatingsMap).flatMap(lambda x: x[1]).countByValue()

genreArtistRatingRows = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseVector).filter(lambda x: x[0] in itemRatingsMap).join(itemRatings).flatMapValues(lambda x:x).flatMapValues(lambda x:x).groupByKey()

avgGenreRating = genreArtistRatingRows.map(lambda x: list(x[1])).map(lambda x: (x[0], float(x[2].encode('ascii', 'ignore')))).groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).map(lambda x: (x[1], x[0]))

avgArtistRating = genreArtistRatingRows.map(lambda x: list(x[1])).map(lambda x: (x[1], float(x[2].encode('ascii', 'ignore')))).groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).map(lambda x: (x[1], x[0]))

avgRating = avgGenreRating.union(avgArtistRating).map(lambda x: (x[1], x[0])).collectAsMap()

broadCastedData = sc.broadcast(avgRating)
recommendationList = songsAttributes.map(calculateScore).takeOrdered(50,key = lambda x: -x[1])
