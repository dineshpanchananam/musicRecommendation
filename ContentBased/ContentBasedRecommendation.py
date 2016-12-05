"""
Content Based Music Recommendation System
"""

import sys
from collections import defaultdict
from itertools import combinations
import numpy as np
import random
import csv
import pdb
from math import log10
from pyspark import SparkConf,SparkContext

"""
Extract song id as a key and it's corresponding genre id 
and artist id as a value from song attributes file.
"""
def parseAttributes(line):
	line = line.split("\t")
	return line[0],(line[3],line[2])

"""
Extract user id as a key and it's corresponding song id 
and rating as a value from input data file.
"""	
def parseInput(line):
	line = line.split("\t")
	return line[0],(line[1],line[2])

"""
Extract song id as a key and it's corresponding song name 
as a value from song names file.
"""	
def separate(line):
	line = line.split("\t")
	return line[0],line[1]

"""
Extract genre id as a key and it's corresponding genre name 
as a value from genre-hierarchy file.
"""
def getGenreName(line):
	line = line.split("\t")
	return line[0],line[3]

"""
Calculate a score for each song in the input dataset based on the ratings for
genre and artists of the songs that current user has previously rated.
"""
def calculateScore(line):
	score = 0.0
	line = line.split('\t')
	artist = line[2]
	genre = line[3]
	if(genre in broadCastedData.value):
		if(genre=='0'):
			score = log10(1+ 0.001)*0.25
		else:
			score += log10(1 + broadCastedData.value[str(genre)])*0.25
	if(artist in broadCastedData.value):
		score += log10(1 + broadCastedData.value[str(artist)])*0.75
	return line[0], score

"""
Get the spark context
"""
conf = SparkConf()
sc = SparkContext(conf = conf)
#sc = SparkContext(sys.argv[1], "PythonUserCF")

"""
Start of execution
"""
if len(sys.argv) > 1:

	#Default number of recommendations set to 50
	number_of_recommendations = 50
	input_file = sys.argv[1]
	input_attributes = sys.argv[2]
	song_names = sys.argv[3]
	user_id = sys.argv[4]
	if len(sys.argv)>5:
		number_of_recommendations = int(sys.argv[5])

	#Read input data file conatining user id, song id and rating
	data = sc.textFile(input_file)
	#Read song attributes file conatining song id, album id, artist id and genre id.
	songsAttributes = sc.textFile(input_attributes)
	#Read song names file conatining song id and song name
	songNames = sc.textFile(song_names)
	#Read genre hierarchy file conatining genre id, parent id, genre level and genre name
	genreNames = sc.textFile("genre-hierarchy.txt")

	# Get item, rating pairs of the songs that current user has rated. [(i1,r1), (i2, r2)...]
	itemRatings = data.map(parseInput).filter(lambda x: x[0] == user_id).map(lambda x : (x[1][0],x[1][1]))
	itemRatingsMap = itemRatings.collectAsMap()

	#Get genre id, artist id and rating for each song that user has rated
	genreArtistRatingRows = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseAttributes).filter(lambda x: x[0] in itemRatingsMap).join(itemRatings).flatMapValues(lambda x:x).flatMapValues(lambda x:x).groupByKey()

	#Calculate average rating per genre from te songs user has rated
	avgGenreRating = genreArtistRatingRows.map(lambda x: list(x[1])).map(lambda x: (x[0], float(x[2].encode('ascii', 'ignore')))).groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).map(lambda x: (x[1], x[0]))

	#Calculate average rating per artist from te songs user has rated
	avgArtistRating = genreArtistRatingRows.map(lambda x: list(x[1])).map(lambda x: (x[1], float(x[2].encode('ascii', 'ignore')))).groupByKey().map(lambda x : (x[0], list(x[1]))).mapValues(sum).map(lambda x: (x[1], x[0]))

	#Keep average genre and average artist ratings together for easy lookup while calculating song score
	avgRating = avgGenreRating.union(avgArtistRating).map(lambda x: (x[1], x[0])).collectAsMap()

	#Broadcast the average ratings dictionary for genres and artists ratings
	broadCastedData = sc.broadcast(avgRating)
	
	#Get the top n recommended songs based on the score calculated in the form (song id, song score)
	recommendationList = songsAttributes.map(calculateScore).takeOrdered(number_of_recommendations,key = lambda x: -x[1])
	
	#Collect the recommended song ids.
	recommendations = sc.parallelize(recommendationList)
	recommendationSongIds = recommendations.map(lambda x:x[0]).collect()

	#Get the names for the recommended song ids
	songWithNames = songNames.map(lambda x:x.encode("ascii","ignore")).map(separate).filter(lambda x:x[0] in recommendationSongIds)
	
	#Get the corresponding genre ids of recommended songs
	recommendationGenreIds = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseAttributes).filter(lambda x: x[0] in recommendationSongIds).map(lambda x: x[1][0]).collect()
	
	#Get names of the genre name of each recommended song based on genre id.
	filteredGenresFromRecommendation = genreNames.map(getGenreName).filter(lambda x: x[0] in recommendationGenreIds).collectAsMap()

	#Get the final result in the form of "'song name' (Genre: 'genre name')" for recommended songs
	songWithGenre = songsAttributes.map(lambda x:x.encode("ascii","ignore")).map(parseAttributes).filter(lambda x: x[0] in recommendationSongIds).map(lambda x: (x[0],filteredGenresFromRecommendation[x[1][0]])).join(songWithNames).collect()

	"""
	Save final result in a file, named 'recommendation-for-(user_id).txt'
	"""
	with open("recommendations-for-%s.txt" % user_id, "w") as o:
	  o.write("\n".join([str(x[1][1])+" (Genre: "+str(x[1][0])+")" for x in songWithGenre]))
	  
	#print [str(x[1][1])+" (Genre: "+str(x[1][0])+")" for x in songWithGenre]
	
else:
	print "check input parameters"
