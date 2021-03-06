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
#### Input Data
The [input data](http://www.cs.colostate.edu/~cs435/datafiles/PA1/Spring2018/CS435PA1Dataset.tar.gz) for PA1 is a dataset compiled from the set of Wikipedia articles. Each data file is formatted as follows:
<pre>… 
Title_of_Article-1<====>DocumentID-1<====>Text_of_Article-1 
NEWLINE 
NEWLINE 
Title_of_Article-2<====>DocumentID-2<====>Text_of_Article-2 
…</pre>
There are 3 components describing an article: (1) Title of the article, (2) Document ID, and (3) Text of the article. The title (text information) represents the title of a Wikipedia article. The document ID is specified by Wikipedia and every article has a unique document id. Finally, the text of the article encapsulates what was included in that Wikipedia article. Each data file may contain multiple Wikipedia articles and these are separated by two consecutive NEWLINE characters.

## PA2
This program extracts the top 3 sentneces that summarize the article using MapReduce. Ranking of the setences is computed using TF.IDF scores; TF.IDF values are computed for each unigram, the top 5 scores per sentecne are summed, giving the Sentence-TF.IDF value.
#### Input Data
The format of the [input data](http://www.cs.colostate.edu/~cs435/datafiles/PA2/PA2Dataset.tar.gz) for PA2 is identical to PA1. Extraneous periods were removed to increase accuracy of sentence parsing.

## PA3
Uses Apache Spark in an iterative fashion to create the following:
#### Estimation of PageRank values under ideal conditions
Creates a sorted list (in descending order) of Wikipedia pages based on their ideal PageRank value. Each row of the output contains the title of the article and its PageRank value. This computation is performed in my own Spark cluster with 5 machines with results from 25 iterations.
#### Estimation of the PageRank values while considering dead-end articles
Creates a sorted list (in descending order) of Wikipedia pages based on their PageRank value with taxation. Each row of the output contains the title of the article and its PageRank value. This computation is performed in my own Spark cluster with 5 machines with results from 25 iterations.
#### Analysis of the above results: Creating a Wikipedia Bomb
Creates a Wikipedia Bomb that returns the "Rocky Mountain National Park" wikipedia page for the search key word "surfing". To do this, I modify the link data file and show that the "Rocky Mountain National Park" page generates the highest PageRank among all of the pages containing "surfing" as part of their titles. 
#### Input Data
PA3 uses Wikipedia dump generated by Henry Haselgrove. The input dataset contains [Wikipedia-Links-Simple-Sorted](http://www.cs.colostate.edu/~cs435/datafiles/Spring2018PA3/links-simple-sorted.zip) and [Wikipedia-Titles-Sorted](http://www.cs.colostate.edu/~cs435/datafiles/Spring2018PA3/titles-sorted.zip). ```links-simple-sorted.txt```, contains one line for each page that has links from it. The format of the lines is as follows:
<pre>from<sub>1</sub>: to<sub>11</sub> to<sub>12</sub> to<sub>13</sub> ... 
from<sub>2</sub>: to<sub>21</sub> to<sub>22</sub> to<sub>23</sub> ... 
...</pre>
where from<sub>1</sub> is an integer labeling a page that has links from it, and to<sub>11</sub> to<sub>12</sub> to<sub>13</sub> ... are integers labeling all the pages that the page links to. To find a page title that corresponds to integer ```n```, just look up the ```n```-th line in the file ```titles-sorted.txt```, a UTF8-encoded text file.
