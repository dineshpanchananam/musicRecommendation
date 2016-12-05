# Item-based Collaborative Filtering on pySpark with cosine similarity
# and weighted sums
import sys
from collections import defaultdict
from itertools import combinations
import numpy as np
import random
import csv
import pdb
from pyspark import SparkConf,SparkContext

def parseVector(line):
    '''
    Parse each line of the specified data file, assuming a "\t" delimiter.
    Converts each rating to a float
    '''
    line = line.split("\t")
    return line[0],(line[1],float(line[2]))

def sampleInteractions(user_id,items_with_rating,n):
    '''
    For users with # interactions > n, replace their interaction history
    with a sample of n items_with_rating
    '''
    if len(items_with_rating) > n:
        return user_id, random.sample(items_with_rating,n)
    else:
        return user_id, items_with_rating
'''
[(u'c3', [(u'i1', 0.0), (u'i2', 2.0), (u'i3', 1.0), (u'i4', 3.0), (u'i5', 0.0)]), (u'c1', [(u'i1', 0.0), (u'i2', 4.0), (u'i3', 2.0), (u'i4', 5.0), (u'i5', 0.0)]), (u'c2', [(u'i4', 4.0), (u'i5', 0.0), (u'i1', 2.0), (u'i2', 3.0), (u'i3', 3.0)]), (u'c4', [(u'i3', 0.0), (u'i4', 0.0), (u'i5', 1.0), (u'i1', 5.0), (u'i2', 0.0)])]
'''        
def findItemPairs(user_id,items_with_rating):
    '''
    For each user, find all item-item pairs combos. (i.e. items with the same user)
    '''
    #for item1,item2 in combinations(items_with_rating,2):
        #return (item1[0],item2[0]), (item1[1],item2[1])
    return [ [(item1[0],item2[0]), (item1[1],item2[1])] for (item1, item2) in combinations(items_with_rating,2)]

def calcSim(item_pair,rating_pairs):
    '''
    For each item-item pair, return the specified similarity measure,
    along with co_raters_count
    '''
    sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
    for rating_pair in rating_pairs:
        sum_xx += np.float(rating_pair[0]) * np.float(rating_pair[0])
        sum_yy += np.float(rating_pair[1]) * np.float(rating_pair[1])
        sum_xy += np.float(rating_pair[0]) * np.float(rating_pair[1])
        #sum_y += rt[1]
        #sum_x += rt[0]
        n += 1
    cos_sim = cosine(sum_xy,np.sqrt(sum_xx),np.sqrt(sum_yy))
    return item_pair, (cos_sim,n)

def cosine(dot_product,rating_norm_squared,rating2_norm_squared):
    '''
    The cosine between two vectors A, B
    dotProduct(A, B) / (norm(A) * norm(B))
    '''
    numerator = dot_product
    denominator = rating_norm_squared * rating2_norm_squared
    return (numerator / (float(denominator))) if denominator else 0.0

def keyOnFirstItem(item_pair,item_sim_data):
    '''
    For each item-item pair, make the first item's id the key
    '''
    (item1_id,item2_id) = item_pair
    return item1_id,(item2_id,item_sim_data)

def nearestNeighbors(item_id,items_and_sims,n):
    '''
    Sort the predictions list by similarity and select the top-N neighbors
    '''
    items_and_sims = list(items_and_sims)
    items_and_sims.sort(key=lambda x: x[1][0],reverse=True)
    return item_id, items_and_sims


def topNRecommendations(user_id,items_with_rating,item_sims,n):
    '''
    Calculate the top-N item recommendations for each user using
    the
    weighted sums method
    '''
    # initialize dicts to store the score of each individual item,
    # since an item can exist in more than one item neighborhood
    totals = defaultdict(int)
    sim_sums = defaultdict(int)
    for (item,rating) in items_with_rating:
        # lookup the nearest neighbors for this item
        nearest_neighbors = item_sims.get(item,None)
        if nearest_neighbors:
            for (neighbor,(sim,count)) in nearest_neighbors:
                if neighbor != item:
                    # update totals and sim_sums with the rating data
                    totals[neighbor] += sim * rating
                    sim_sums[neighbor] += sim
    # create the normalized list of scored items
    scored_items = [(total/sim_sums[item],item) for item,total in totals.items()]
    # sort the scored items in ascending order
    scored_items.sort(reverse=True)
    # take out the item score
    # ranked_items = [x[1] for x in scored_items]
    return user_id,scored_items[:n]

def separate(line):
	line = line.split("\t")
	return line[0],line[1]
############################## START#########################################
if len(sys.argv) < 4:
	print >> sys.stderr, \
	"Usage: PythonUserCF <master> <file>"
	exit(-1)

conf = SparkConf()
sc = SparkContext(conf = conf)
lines = sc.textFile(sys.argv[1])
song_names = sys.argv[2]
#lines = sc.textFile("ydata-ymusic-rating-study-v1_0-train.txt")
'''
Obtain the sparse user-item matrix:
user_id -> [(item_id_1, rating_1),
[(item_id_2, rating_2),
...]
'''
user_item_pairs = lines.map(parseVector).groupByKey().map(
lambda p: sampleInteractions(p[0],p[1],500)).cache()
'''
Get all item-item pair combos:
(item1,item2) -> [(item1_rating,item2_rating),
(item1_rating,item2_rating),
...]
'''

pairwise_items = user_item_pairs.filter(
lambda p: len(p[1]) > 1).map(
lambda p: findItemPairs(p[0],p[1])).flatMap(lambda x:x).groupByKey()

'''
Calculate the cosine similarity for each item pair and select the
top-N nearest neighbors:
(item1,item2) -> (similarity,co_raters_count)
'''
item_sims = pairwise_items.map(
lambda p: calcSim(p[0],p[1])).map(
lambda p: keyOnFirstItem(p[0],p[1])).groupByKey().map(
lambda p: nearestNeighbors(p[0],p[1],50)).collect()
'''
Preprocess the item similarity matrix into a dictionary and store
it as a broadcast variable:
'''
item_sim_dict = {}
for (item,data) in item_sims:
    item_sim_dict[item] = data
    isb = sc.broadcast(item_sim_dict)
'''
Calculate the top-N item recommendations for each user
user_id -> [item1,item2,item3,...]
'''
user_item_recs = user_item_pairs.map(
lambda p: topNRecommendations(p[0],p[1],isb.value,50))


'''
Display top N recommendations for a given user
'''
user_id = sys.argv[3]
#user_id = '1'
inner_list = sc.parallelize(user_item_recs.filter(lambda x:x[0]==user_id).values().collect()[0])
song_ids = inner_list.map(lambda x:x[1]).collect()

songNames = sc.textFile(song_names)

songWithNames = songNames.map(lambda x:x.encode("ascii","ignore")).map(separate).filter(lambda x:x[0] in song_ids).map(lambda x:x[1]).collect()

"""
Save final result in a file, named 'recommendation-for-(user_id).txt'
"""
with open("item-recommendation-for-%s.txt" % user_id, "w") as o:
	o.write("\n".join([str(x) for x in songWithNames]))

