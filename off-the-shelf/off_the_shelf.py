from pyspark import SparkContext
from pyspark.mlib.recommendation import ALS,\
  MatrixFactorizationModel 

import sys

def parseText(line):
  tokens = line.strip().split("\t")
  return (int(tokens[0]), int(tokens[1]), float(tokens[2]))

if len(sys.argv) > 4:
  
  data_file = sys.argv[1]
  song_names = sys.argv[2]
  recommendations = int(sys.argv[3])
  user_id = int(sys.argv[4])
  
  sc = SparkContext(appName="spark-mlib-als")
  data_set = sc.textFile(data_file).map(parseText)
  items = data_set.map(lambda row: row[1]).distinct()
  user_items = data_set.filter(lambda row: row[0] == user_id)\
                       .map(lambda row: row[1]).collect()
  
  not_rated_items = items.filter(lambda item: item not in user_items)
  model = ALS.train(data_set, rank=10, iterations=5)
  result = not_rated_items.map(lambda not_rated_item: \
                      (not_rated_item, model.predict(user_id, not_rated_item)))\
    .takeOrdered(recommendations, lambda x: -x[1])\
    .map(lambda x: x[0]).collect()
  
  with open("off_the_shelf_recs_for_%d.txt" % user_id, "w") as f:
    f.write("\n".join(result))
  
  

else:
  print """I need:\
  A train set
  A test set 
  number of recommendations
  user-id"""
