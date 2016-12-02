import sys
from collections import defaultdict
from itertools import combinations
import numpy as np
import random
import csv
import pdb
from math import log10
from pyspark import SparkConf,SparkContext

def parseAttributes(line):
	line = line.split("\t")
	return line[0],(line[3],line[1])
	
def parseVector(line):
	line = line.split("\t")
	return line[0],(line[1],line[2])
	
def separate(line):
	line = line.split("\t")
	return line[0],line[1]
	
def getGenreName(line):
	line = line.split("\t")
	return line[0],line[3]

def calculateScore(line):
	score = 0.0
	line = line.split('\t')
	artist = line[1]
	genre = line[3]
	if(genre in broadCastedData.value):
		if(genre=='0'):
			score = log10(1+ 0.001)*0.25
		else:
			score += log10(1 + broadCastedData.value[str(genre)])*0.25
	if(artist in broadCastedData.value):
		score += log10(1 + broadCastedData.value[str(artist)])*0.75
	return line[0], score
	
conf = SparkConf()
sc = SparkContext(conf = conf)
#sc = SparkContext(sys.argv[1], "PythonUserCF")
if len(sys.argv) > 1:

	number_of_recommendations = 50
	input_file = sys.argv[1]
	input_attributes = sys.argv[2]
	song_names = sys.argv[3]
	user_id = sys.argv[4]
	if len(sys.argv)>5:
		number_of_recommendations = int(sys.argv[5])

	data = sc.textFile(input_file)
	songsAttributes = sc.textFile(input_attributes)
	songNames = sc.textFile(song_names)
	genreNames = sc.textFile("genre-hierarchy.txt")

	# [(i1,r1), (i2, r2)...] for logged in user
	itemRatings = data.map(parseVector).filter(lambda x: x[0] == user_id).map(lambda x : (x[1][0],x[1][1]))
	itemRatingsMap = itemRatings.collectAsMap()

	genreArtistRatingRows = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseAttributes).filter(lambda x: x[0] in itemRatingsMap).join(itemRatings).flatMapValues(lambda x:x).flatMapValues(lambda x:x).groupByKey()

	avgGenreRating = genreArtistRatingRows.map(lambda x: list(x[1])).map(lambda x: (x[0], float(x[2].encode('ascii', 'ignore')))).groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).map(lambda x: (x[1], x[0]))

	avgArtistRating = genreArtistRatingRows.map(lambda x: list(x[1])).map(lambda x: (x[1], float(x[2].encode('ascii', 'ignore')))).groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).map(lambda x: (x[1], x[0]))

	avgRating = avgGenreRating.union(avgArtistRating).map(lambda x: (x[1], x[0])).collectAsMap()

	broadCastedData = sc.broadcast(avgRating)
	recommendationList = songsAttributes.map(calculateScore).takeOrdered(number_of_recommendations,key = lambda x: -x[1])
	recommendations = sc.parallelize(recommendationList)
	recommendationSongIds = recommendations.map(lambda x:x[0]).collect()

	songWithNames = songNames.map(lambda x:x.encode("ascii","ignore")).map(separate).filter(lambda x:x[0] in recommendationSongIds)
	recommendationGenreIds = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseAttributes).filter(lambda x: x[0] in recommendationSongIds).map(lambda x: x[1][0]).collect()
	filteredGenresFromRecommendation = genreNames.map(getGenreName).filter(lambda x: x[0] in recommendationGenreIds).collectAsMap()

	songWithGenre = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseAttributes).filter(lambda x: x[0] in recommendationSongIds).map(lambda x: (x[0],filteredGenresFromRecommendation[x[1][0]])).join(songWithNames).collect()

	print [str(x[1][1])+" (Genre: "+str(x[1][0])+")" for x in songWithGenre]
else:
	print "check input file"
