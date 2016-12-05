Off - the - Shelf .Music Recommendation .System
======================================
Main file: off-the-shelf.py

Features
========
1) It's Time complexity = O(|s|) |s| -> number of ratings

2) Linearly scalable


I/O
===

takes 4 arguments
----------------
data-set

music-names-file

number_of_recommendations - may be 25 - 50

user_id - id of the logged in user.

outputs
-------
off-the-shelf-recommendations-for-user_id.txt which contains the song-names

Summary
-----
`spark-submit off_the_shelf.py <data-set> <song-names> <number-of-recommendations> <user-id>`
