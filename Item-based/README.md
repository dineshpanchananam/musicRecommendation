README
Item Based Music Recommendation System
Execution Instructions:
1) The execution requires 3 files-
1.      item-based.py
(The source code file submitted)
2.      Input data file
3.      song_names.txt
(The song names contains rows of form ‘song id<TAB>song name’)
2) INPUT 
This implementation targets the R3 dataset from the following link:
     http://webscope.sandbox.yahoo.com/catalog.php?datatype=r
3) COMMAND 
The code can be executed using spark-submit command.
spark-submit item-based.py <data-set> <song-names> <user-id>
ex. spark-submit item-based.py ydata-ymusic-rating-study-v1_0-train.txt song_names.txt 2
where,
Argument 1: Source code file name (e.g., based.py submitted in the zip)
Argument 2: Input data file name (e.g.,ydata-ymusic-rating-study-v1_0-train.txt from yahoo dataset)
Argument 3: song names file name (e.g., song_names.txt submitted in the zip) 
Argument 4: valid user id for which songs are to be recommended. (e.g., user id is 1)
All these files must be in the working directory.

4) OUTPUT

Output of the progam will be saved in item-recommendation-for-<user-id> file which will contain top 50 songs as a recommendation list in the working directory.

