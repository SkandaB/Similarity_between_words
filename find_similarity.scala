/*
	Spark program in Scala to compute similarity between words using G2 equation
	Variables in equation G2, correspond to the following:
		a = occurances of word1 in document1
		b = occurances of word2 in document2
		c = total number of words in document1
		d = total number of words in document2
		const_ac = a*(ln(a) - ln(c))
		const_bd = b*(ln(b) - ln(d))
		const_cd = ln(c+d)	

	First, read a text file, filter words which contain only digits, remove all punctuations and convert to lower case
	Next, for each word, get the frequency of occurance in respective documents
	Using this, get the total number of words by summing all values
	Next, compute the value ln(total_words) for respective documents
	Broadcast all three values, c, d, ln(c+d)

	For each counts variable, pre-process the values a, const_ac
	Get the Cartesian product of these two pre-processed documents
	Compute the similarity score by substituting values a, b, const_ac, const_bd and const_cd in equation G2

	Store this final result in a file on HDFS

	Ran in a shell, somehow mvn wouldn't let me compiple the .scala file (it's not this file, it's the other file)
*/

// Reads file into alias myfile from the specified HDFS location
val myfile1 = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/similarity/data/data1")
val myfile2 = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/similarity/data/data2")

// Define a function to calculate natural-log of a number
def loge(m : Double) :Double = scala.math.log(m)

// Filter out non-letter words, remove punctuation and get the frequency of each word in the document
val counts1 = myfile1.flatMap(line => line.filterNot(_.isDigit).replaceAll("""[\p{Punct}]""", "").toLowerCase.split(" ")).map(word => (word,1)).reduceByKey(_ + _)
val counts2 = myfile2.flatMap(line => line.filterNot(_.isDigit).replaceAll("""[\p{Punct}]""", "").toLowerCase.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

// For each document get the total number of words in the document by summing the value part above (counts1, counts)
val total_words1 :Double = counts1.values.sum()
val total_words2 :Double = counts2.values.sum()

// Compute the value of const_cd
val const_cd = loge(broadcast_total1.value + broadcast_total2.value)

// Broadcast const_cd, c, d
// The advantage is that spark will copy these values to each node only once.
val broad_const_cd = sc.broadcast(const_cd)
val broadcast_total1 = sc.broadcast(total_words1)
val broadcast_total2 = sc.broadcast(total_words2)


// Pre-process the documents and compute const_ac and const_bd
val pre_proc1 = counts1.map(x => (x._1, x._2, x._2*(loge(x._2) - loge(broadcast_total1.value))))
val pre_proc2 = counts2.map(x => (x._1, x._2, x._2*(loge(x._2) - loge(broadcast_total2.value))))

// Join the above two pre-processed values using Cartesian product and remove records where word1 = word2
val cross = pre_proc1.cartesian(pre_proc2).filterNot(x => x._1._1 = x._2._1)

// Compute the similarity score between words using G2 euqation
val similarity_score = cross.map(x => (x._1._1, x._2._1, 2*((x._1._2 + x. _2._2) * (broad_const_cd.value - loge(x._1._2 + x._2._2)) + x._1._3 + x._2._3)))

// Save the output to HDFS
similarity_score.saveAsTextFile("hdfs://localhost:8020/user/cloudera/Similarity/result1")
