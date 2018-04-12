# Big-Data

## PA0
Basic WordCount MapReduce program to verify our Hadoop was set up correctly. 

## PA1
Generates the following unigram profiles (from a 1G dataset of Wikipedia articles) using MapReduce:
#### Profile 1
A list of unigrams that occurred at least once in the entire corpus. The unigrams are sorted in (ascending) alphabetical order. No duplicates.
#### Profile 2
A list of unigrams and their frequencies within the target article. This profile is generated per article. The resulting list is grouped by the Document ID, and sorted (in descending order) on the frequency of the unigram within the article.
#### Profile 3
A list of unigrams and their frequencies within the corpus. The list of unigrams are sorted (in descending order) on the frequency of the unigram within the corpus.

## PA2
Given the same dataset as PA1, this program extracts the top 3 sentneces that summarize the article using MapReduce. Ranking of the setences is computed using TF.IDF scores; TF.IDF values are computed for each unigram, the top 5 scores per sentecne are summed, giving the Sentence-TF.IDF value.

## PA3
Uses Apache Spark in an iterative fashion to create the following:
#### Estimation of PageRank values under ideal conditions
Creates a sorted list (in descending order) of Wikipedia pages based on their ideal PageRank value. Each row of the output contains the title of the article and its PageRank value. This computation is performed in my own Spark cluster with 5 machines with results from 25 iterations.
#### Estimation of the PageRank values while considering dead-end articles
Creates a sorted list (in descending order) of Wikipedia pages based on their PageRank value with taxation. Each row of the output contains the title of the article and its PageRank value. This computation is performed in my own Spark cluster with 5 machines with results from 25 iterations.
#### Analysis of the above results: Creating a Wikipedia Bomb
Creates a Wikipedia Bomb that returns the "Rocky Mountain National Park" wikipedia page for the search key word "surfing". To do this, I modify the link data file and show that the "Rocky Mountain National Park" page generates the highest PageRank among all of the pages containing "surfing" as part of their titles. 
