from pyspark import SparkContext
import sys
from math import sqrt

tab = "\t"
comma = ","
POWER_USERS = 15

count = 0

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
	count = 0
	for (user, ratings) in power_users:
		for (item_id, rating) in ratings:
			if item_id == item:
				score += similarity_vector[user] * rating

	return score




def pearson_correlation():
	pass


def compute_similarity_vector(ratings, power_users):
	# decide similarity from ratings
	# for now we use cosine
	function = cosine_similarity
	return sorted(map(lambda power_user: [power_user[0], function(ratings, power_user[1])], power_users),
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


if len(sys.argv) > 1:
  input_file = sys.argv[1]
  sep = comma if ".csv" in input_file else tab
  sc = SparkContext(appName="user-based")

  file_input = sc.textFile(input_file)\
         .map(lambda line: line.strip().split(sep))\
         .map(lambda row: (int(row[0]), (int(row[1]), float(row[2]))))

  items = file_input.map(lambda row: row[1]).groupByKey().map(lambda x: x[0])
  
  users = file_input.groupByKey()\
         .map(lambda row: (row[0], row[1], sqrt(sum(map(lambda rating: rating[1] * rating[1], row[1])))))\
         .map(lambda row: (row[0], map(lambda rating: (rating[0], rating[1] / (0.000001 + row[2])), row[1])))

  power_users = users.takeOrdered(POWER_USERS, key=lambda x: -len(x[1]))

  user_id = 10
  # get top 10 items
  N = 10
  print item.count()

  user  = users.filter(lambda x: x[0] == user_id).first()
  user_items = [item[0] for item in user[1]]
  similarity_vector = dict(compute_similarity_vector(user[1], power_users))
  not_rated = items.filter(lambda item: item not in user_items)
  recommendations = not_rated.map(lambda item: (item, predict_rating(item, power_users, similarity_vector)))\
  			.takeOrdered(N, lambda x: -x[1])
  print recommendations

  '''
  similarity_matrix = users.map(lambda user: [user[0], compute_similarity_vector(user[1], power_users)])

  '''

  """
  top_items = items.map(lambda item: predict_rating(item, power_users))
  print top_items.take(1) """


else:
  print "check input file"
