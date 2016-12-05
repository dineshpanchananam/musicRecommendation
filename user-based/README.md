User - based .Music Recommendation .System
======================================
Main file: user-based.py

Features
========
1) It's Time complexity = O(|s|) |s| -> number of songs

2) Linearly scalable

3) Can switch similarity function

I/O
===

takes 6 arguments
----------------
data-set (text.txt)

music-names-file (names_.txt)

similarity-function - can be (cosine | pearson)

power_users - no of popular visitors to be compared against

number_of_recommendations - may be 25 - 50

user_id - id of the logged in user.

outputs
-------
user-based-recommendations-for-user_id.txt which contains the song-names

Summary
-----
`spark-submit user_based.py <data-set> <song-names> <similarity-fun -> cosine | pearson> <power_users> <number-of-recommendations> <user-id>`
