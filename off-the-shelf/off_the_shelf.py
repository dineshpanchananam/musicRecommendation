######################################
# Author: Dinesh Panchananam (Group 11)
# UNCC Id: 800937153
# email: dpanchan@uncc.edu
######################################

from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import sys

# this function converts
# a tab seperated line to 
# int, int, float
def parseText(line):
  tokens = line.strip().split("\t")
  return (int(tokens[0]), int(tokens[1]), float(tokens[2]))


if len(sys.argv) > 4:
  # only if we have all the requirements
  # input data-set
  data_file = sys.argv[1]
  # song names file
  song_names_file = sys.argv[2]
  # number of recommendations
  recommendations = int(sys.argv[3])
  # id of the logged in user
  user_id = int(sys.argv[4])
  # create a spart context
  sc = SparkContext(appName="spark-mlib-als")
  # parse songs
  song_names = sc.textFile(song_names_file)\
                 .map(lambda line: line.strip().split("\t"))\
                 .map(lambda tup: (int(tup[0]), tup[1]))\
                 .collectAsMap()

  # parse input data set
  data_set = sc.textFile(data_file).map(parseText).cache()
  # items - ids of all the songs
  items = data_set.map(lambda row: row[1]).distinct()
  # items which are rated by user
  user_items = data_set.filter(lambda row: row[0] == user_id)\
                       .map(lambda row: row[1]).collect()
  # items not rated by user
  not_rated_items = items.filter(lambda item: item not in user_items).collect()
  # Spark magic in the background
  model = ALS.train(data_set, rank=1, iterations=1)
  # list of (song, predictive rating) for that user
  result = model.predictAll(sc.parallelize(map(lambda item: (user_id, item), not_rated_items)))\
                .map(lambda x: (x[1], x[2]))\
                .takeOrdered(recommendations, key = lambda x: -x[1])

  # writing to output
  # one line for a recommendation
  with open("off-the-shelf-recommendations-for-%d.txt" % user_id, "w") as o:
    o.write("\n".join(map(lambda row: song_names.get(row[0], "N/A"), result)))

  # stop the context to avoid resource leak
  sc.stop()
else:
  print "I need <data-set> <song-names> <number-of-recommendations> <user-id>"
