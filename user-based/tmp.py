from pyspark import SparkContext
import sys
from math import sqrt

tab = "\t"
comma = ","
POWER_USERS = 10
similarity_function = "cosine"

def cosine_similarity(ratings1, ratings2):
    # ratings1 => [(item1<int>,.rating<float>)]
    # ratings2 => [(item2<int>, rating<float>)]
    m, n = len(ratings1), len(ratings2)
    i, j, score = 0, 0, 0.0
    while i < m and j < n:
        if ratings1[i][0] == ratings2[j][0]:
            score += ratings1[i][1] * ratings2[j][1]
            i += 1
            j += 1
        elif ratings1[i][0] < ratings2[j][0]:
            i += 1
        else:
            j += 1

    return score

def predict_rating(item, power_users, similarity_vector):
    score = 0.0
    normalizing_factor = 0
    for (user, ratings) in power_users:
        for (item_id, rating) in ratings:
            if item_id == item:
                score += similarity_vector[user] * rating
                normalizing_factor += 1

    return score / max(normalizing_factor, 1)


def pearson_correlation(ratings1, ratings2):
  # ratings1 => [(item1<int>,.rating<float>)]
    # ratings2 => [(item2<int>, rating<float>)]
  score = 0.0
  items1 = set([x[0] for x in ratings1])
  items2 = set([x[0] for x in ratings2])
  avg_rating1 = sum(map(lambda x: x[1], ratings1)) / len(ratings1)
  avg_rating2 = sum(map(lambda x: x[1], ratings2)) / len(ratings2)
  ratings1_map = dict(ratings1)
  ratings2_map = dict(ratings2)
  common_items = items1.intersection(items2)
  if common_items:
    for common_item in common_items:
      rating_by_1 = ratings1_map.get(common_item, 0) # r(x,s)
      rating_by_2 = ratings2_map.get(common_item, 0) # r(y,s)
      a = rating_by_1 - avg_rating1
      b = rating_by_2 - avg_rating2
      denom = sqrt(a * a + b * b)
      denom = max(denom, 1.0)
      score += (a * b) / denom
  
  return score

def compute_similarity_vector(ratings, power_users):
    # decide similarity from ratings
    # for now we use cosine
    global similarity_function
    return sorted(map(lambda power_user: [power_user[0], similarity_function(ratings, power_user[1])], power_users),
        key = lambda x: -x[1])


'''
transformations
users:
line -> (user, item, rating)
-> (int, int, float)
-> (int, [(int, float)])
-> also normalize

power-users
(int, [(int, float)])
-> (int, int) =~ (user-id, number of ratings)



'''


if len(sys.argv) >= 7:
  input_file = sys.argv[1]
  song_names_file = sys.argv[2]
  similarity_function = {"cosine": cosine_similarity, "pearson": pearson_correlation }.get(sys.argv[3], "cosine")
  POWER_USERS = int(sys.argv[4])
  N = int(sys.argv[5])
  user_id = int(sys.argv[6])
  
  sep = comma if ".csv" in input_file else tab
  sc = SparkContext(appName="user-based")

  song_names = sc.textFile(song_names_file)\
                 .map(lambda line: line.strip().split(tab))\
                 .map(lambda tup: (int(tup[0]), tup[1]))\
                 .collectAsMap()

  file_input = sc.textFile(input_file)\
         .map(lambda line: line.strip().split(sep))\
         .map(lambda row: (int(row[0]), (int(row[1]), float(row[2]))))

  items = file_input.map(lambda row: row[1][0]).distinct()
  
  users = file_input.groupByKey()\
         .map(lambda row: (row[0], row[1], sqrt(sum(map(lambda rating: rating[1] * rating[1], row[1])))))\
         .map(lambda row: (row[0], map(lambda rating: (rating[0], rating[1] / (0.000001 + row[2])), row[1])))

  power_users = users.takeOrdered(POWER_USERS, key=lambda x: -len(x[1]))
  
  user  = users.filter(lambda x: x[0] == user_id).first()
  user_items = [item[0] for item in user[1]]
  similarity_vector = dict(compute_similarity_vector(user[1], power_users))
 
  not_rated = items.filter(lambda item: item not in user_items)
 
  recommendations = not_rated.map(lambda item: (item, predict_rating(item, power_users, similarity_vector)))\
            .takeOrdered(N, lambda x: -x[1])

  with open("user-based-recommendations-for-%d.txt" % user_id, "w") as o:
    o.write("\n".join(map(lambda reco: song_names.get(reco[0], "title N/A"), recommendations)))

else:
  print "input-file song-names-file power-users number-of-recommendations user-id"

