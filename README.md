# MovieRecommendation

Link to the Web Page:
http://webpages.uncc.edu/~snagiset/cloudreport.html

Instructions to run the code:

Content based recommendation system:
$ mkdir pagerank_classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* FinalContent.java -d pagerank_classes -Xlint
jar -cvf recom.jar -C pagerank_classes/ .
hadoop jar recom.jar org.myorg.FinalContent /user/vkundula/input1(movies.csv)  /user/vkundula/input2(ratings.csv)
The final output will be in collaboutput6


Collaborative based recommendation system:
$ mkdir pagerank_classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* FinalCollaborative.java -d pagerank_classes -Xlint
jar -cvf recom.jar -C pagerank_classes/ .
hadoop jar recom.jar org.myorg.FinalCollaborative /user/vkundula/input(ratings.csv)
The final output will be in contentoutput6



Hybrid:
$ mkdir pagerank_classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Hybrid.java -d pagerank_classes -Xlint
jar -cvf recom.jar -C pagerank_classes/ .
hadoop jar recom.jar org.myorg.Hybrid /user/vkundula/input1(contentoutput.txt) /user/vkundula/input2(collaborativeoutput.txt) /user/vkundula/output


Notes:
All the output files have been provided with the submission
All the other details have been provided in the webpage


Dataset:
The dataset used is movielens dataset with over 1 million data items

Framework used:
we used Hadoop MapReduce for the project

Fun thing about project:
It is very interesting to know how we generally get the recommendations in idbm and netflix with the help of this project
