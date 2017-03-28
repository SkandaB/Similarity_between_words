/* 
  1. Read the two pre-processed files for two documents
  2. Calculate LOG(c+d) once for the entire document. c = total words in document1, d = total words in document2
  3. FULL outer join two documents and create pair-wise records with their corresponding information
  4. Emit the following information for record
	word1, word2, a, b, log(c+d)
*/

-- Setting the name of the job to debug in the applications
SET job.name 'join_and_calculate_sim'


-- Read the pre-processed file1 into doc1 alias
doc1 = LOAD '$pre_processed_file1' using PigStorage(',') as (word1:chararray, a_freq:float, doc1_total:float, ln_ac:float);
DESCRIBE doc1;


-- Read the pre-processed file2 into doc2 alias
doc2 =  LOAD '$pre_processed_file2' using PigStorage(',') as (word2:chararray, b_freq:float, doc2_total:float, ln_bd:float);
DESCRIBE doc2;


-- Create the Cartesian Product of these two documents. Output will have the columns: word1, num_of_occurrences of word1, total_words in doc1, pre_computed value a(log(a)-log(c)),  word2, num_of_occurrences of word2, total_words in doc2, pre_computed value b(log(b)-log(d))
doc1_doc2_cross = CROSS doc1, doc2;

-- Filter out the words where both words are same
doc1_doc2 = FILTER doc1_doc2_cross BY doc1::word1 != doc2::word2;

-- Calculate the value of Log(c+d) only once and use it later
first_cross_record = LIMIT doc1_doc2 1;
log_cd = FOREACH first_cross_record GENERATE LOG(doc1::doc1_total + doc2::doc2_total);

-- Using the pre_processed values involving a, b, c and d, calculate the similarity between two words on each line, Output will contain columns word1, word2, similarity score
sim_word1_word2 = FOREACH doc1_doc2  GENERATE doc1::word1, doc2::word2, 2*(((doc1::a_freq + doc2::b_freq)*(log_cd.$0 - LOG(doc1::a_freq + doc2::b_freq))) + doc1::ln_ac + doc2::ln_bd);

-- Store the final result in a file on HDFS
STORE sim_word1_word2 INTO '$sim_result' using PigStorage(',');
