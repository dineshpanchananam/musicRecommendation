######################################
# Author: Dinesh Panchananam (Group 11)
# UNCC Id: 800937153
# email: dpanchan@uncc.edu
######################################

from pyspark import SparkContext
import sys
from math import sqrt

# convenience
tab = "\t"
comma = ","

# power users - users who rated most number of items
POWER_USERS = 10
# similarity function - can be "cosine" or "pearson"
similarity_function = "cosine"


# the cosine function
# input - two normalized ratings vectors 
# output - float ~> similarity of the vectors
def cosine_similarity(ratings_1, ratings_2):
  # ratings1 => [(item1<int>,.rating<float>)]
  # ratings2 => [(item2<int>, rating<float>)]
  # they are to be sorted to have an O(|n|) complexity overall
  ratings1 = sorted(ratings_1, key=lambda x: x[0])
  ratings2 = sorted(ratings_2, key=lambda x: x[0])
  m, n = len(ratings1), len(ratings2)
  # following the linkedlist technique
  # to find the common songs
  i, j, score = 0, 0, 0.0
  # i, j are used as iterators
  # score contains the similarity 
  while i < m and j < n:
    if ratings1[i][0] == ratings2[j][0]:
      # match found!
      score += ratings1[i][1] * ratings2[j][1]
      # move both
      i += 1
      j += 1
    elif ratings1[i][0] < ratings2[j][0]:
      i += 1
    else:
      j += 1
      
  return score

# the pearson function
# to be used for similarity score between two users
# input - normalized rating vectors of two users
def pearson_correlation(ratings1, ratings2):
  # ratings1 => [(item1<int>,.rating<float>)]
  # ratings2 => [(item2<int>, rating<float>)]
  # score contains the similarity score
  score = 0.0
  # convert to set so that intersection becomes easier
  items1 = set([x[0] for x in ratings1])
  items2 = set([x[0] for x in ratings2])
  # average of all the ratings by user 1
  avg_rating1 = sum(map(lambda x: x[1], ratings1)) / len(ratings1)
  # average of all the ratings by user 2
  avg_rating2 = sum(map(lambda x: x[1], ratings2)) / len(ratings2)
  # map'em so that we have the cool O(1) access
  ratings1_map = dict(ratings1)
  ratings2_map = dict(ratings2)
  # the logic starts with finding common items
  common_items = items1.intersection(items2)
  # if empty, no similarity, relatively neutral -> 0.0!
  if common_items:
    for common_item in common_items:
      # rating of that common_item by user 1
      rating_by_1 = ratings1_map.get(common_item, 0) # r(x,s)
      # rating of that common_item by user 2
      rating_by_2 = ratings2_map.get(common_item, 0) # r(y,s)
      # deviation from mean 
      a = rating_by_1 - avg_rating1
      # deviation from mean 
      b = rating_by_2 - avg_rating2
      denom = sqrt(a * a + b * b)
      denom = max(denom, 1.0)
      # core logic
      score += (a * b) / denom
  
  return score


# this function predicts
# the probable rating of <item> by 
# a user based on his <similarity> with
# <power_users>
# this used weighted average
def predict_rating(item, power_users, similarity_vector):
  # score contains the probable rating
  score = 0.0
  # normalizing factor for weighted average
  normalizing_factor = 0
  # for every power user
  for (user, ratings) in power_users:
    # for every item, (s)he has rated
    for (item_id, rating) in ratings:
      # if that matches with the current item
      # -- which has to be rated
      if item_id == item:
        # score += similarity with that power user * rating by that power user
        score += similarity_vector[user] * rating
        # also increment the size
        normalizing_factor += 1
  
  # / max(...) to avoid 0 / 0
  return score / max(normalizing_factor, 1)

# function that generates
# the similarity vector
# for a user
def compute_similarity_vector(ratings, power_users):
    # decide similarity from ratings
    # for now we use cosine
    global similarity_function
    return sorted(map(lambda power_user: [power_user[0], similarity_function(ratings, power_user[1])], power_users), key = lambda x: -x[1])


'''
transformations
===============
from file pickup line
line -> (user, item, rating)
-> (int, int, float)
-> (int, [(int, float)])
-> groupByKey 0
-> also normalize
-> user-item matrix ready

power-users
============
(int, [(int, float)])
-> (int, int) =~ (user-id, number of ratings)
-> take top users with descending number of ratings
'''


if len(sys.argv) >= 7:
  # unless we have all the requirements, never start
  # the data set
  input_file = sys.argv[1]
  # song names file
  song_names_file = sys.argv[2]
  # choose between cosine and pearson
  # defaults to cosine on error
  similarity_function = {"cosine": cosine_similarity, "pearson": pearson_correlation }.get(sys.argv[3], "cosine")
  # the number of popular users to be compared against
  POWER_USERS = int(sys.argv[4])
  # the number of intended recommendations
  N = int(sys.argv[5])
  # the logged in user who needs recommendations
  user_id = int(sys.argv[6])
  
  # switch to , if csv
  sep = comma if ".csv" in input_file else tab
  # initiate a Spark Context
  sc = SparkContext(appName="user-based")
  # song name processing
  # line ->
  # song_id song_name ->
  # <int> <songname>
  # map
  song_names = sc.textFile(song_names_file)\
                 .map(lambda line: line.strip().split(tab))\
                 .map(lambda tup: (int(tup[0]), tup[1]))\
                 .collectAsMap()
  
  # data set processing
  # line ->
  # user_str, item_str, rating_str ->
  # user_int, [item_int, rating_float]
  file_input = sc.textFile(input_file)\
         .map(lambda line: line.strip().split(sep))\
         .map(lambda row: (int(row[0]), (int(row[1]), float(row[2]))))

  # get the ids of all the songs
  items = file_input.map(lambda row: row[1][0]).distinct()
  
  # group by key on file_input ->
  # user - song - matrix ->
  # then normalize, using 0.000001 to avoid 0 / 0
  users = file_input.groupByKey()\
         .map(lambda row: (row[0], row[1], sqrt(sum(map(lambda rating: rating[1] * rating[1], row[1])))))\
         .map(lambda row: (row[0], map(lambda rating: (rating[0], rating[1] / (0.000001 + row[2])), row[1])))
  
  # take top POWER_USERS to be compared against
  power_users = users.takeOrdered(POWER_USERS, key=lambda x: -len(x[1]))
  
  # get the current user from the matrix
  user  = users.filter(lambda x: x[0] == user_id).first()
  # his/her song-ratings
  user_items = [item[0] for item in user[1]]
  # similarity map for a cool O(1) access
  similarity_vector = dict(compute_similarity_vector(user[1], power_users))
  # songs that are not rated by the user
  not_rated = items.filter(lambda item: item not in user_items)
  # generated recommendations
  # algorithm described in README

  recommendations = not_rated.map(lambda item: (item, predict_rating(item, power_users, similarity_vector)))\
            .takeOrdered(N, lambda x: -x[1])

  # stop the context to avoid resource leaks
  sc.stop()
  
  # write the output to a file
  # one line for a recommendation
  with open("user-based-recommendations-for-%d.txt" % user_id, "w") as o:
    o.write("\n".join(map(lambda reco: song_names.get(reco[0], "title N/A"), recommendations)))

else:
  # error message  
  print "input-file song-names-file similarity-function[cosine|pearson| power-users number-of-recommendations user-id"
