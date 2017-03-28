/*
  This is a brute force approach to find similarity between two words accross documents
  Load both documents
  Calculate the stats for each word in their respective documents
  Use rudimentary method (wc -w) to find the total number of words in respective documents and hard-code into code
  Having a = freq. of word1 in corpus1, b = freq. of word2 in corpus2, c = total number of words in corpus1, d = total number of words in corpus2
  We define similarity between two words uinsg the Log-likelihood equation G2.

  G2 = 2*((a*ln (a/E1)) + (b*ln (b/E2)))

  where E1 = c*(a+b)/(c+d) and E2 = d*(a+b)/(c+d)
*/

-- Setting the name of the job to debug in the applications
SET job.name 'brute_force_approach';

-- Reading both corpus1 and corpus2 into two different pipes (alias)
read_file1 = LOAD 'self_check1/data1'  USING TextLoader as (line:chararray);
read_file2 = LOAD 'self_check2/data2'  USING TextLoader as (line:chararray);

-- Computing the frequency of each word in both corpora
flatten_words1 = FOREACH read_file1 GENERATE FLATTEN(TOKENIZE(line));
flatten_words2 = FOREACH read_file2 GENERATE FLATTEN(TOKENIZE(line));

-- Group by word in each document
grp_by_word1 = GROUP flatten_words1 by $0;
grp_by_word2 = GROUP flatten_words2 by $0;

-- Calculate frequency of each word in correcponding documents
freq1 = FOREACH grp_by_word1 GENERATE $0, COUNT($1);
freq2 = FOREACH grp_by_word2 GENERATE $0, COUNT($1);

-- Get the Cartesian product of document1 x document2
cartesian_product = CROSS freq1, freq2;
describe cartesian_product;

-- Calculate Similarity between words across two documents
sim_word1_word2 = FOREACH cartesian_product
		  GENERATE $0, $2,
		  2*( ($1*(LOG($1/(30*($1+$3)/(30+21))))) + ($3*(LOG($3/(21*($1+$3)/(30+21))))) );

/*
The above formula re-written here to read easily
G2 = 2*
( 
	(
		$1*(
				LOG(
					$1/(30*($1+$3)/(30+21))
				    )
		    )
	) 
    + 
	(
		$3*(
				LOG(
					$3/(21*($1+$3)/(30+21))
				    )
		    )
	)
);
*/

-- Store the reeult as a file in HDFS
STORE sim_word1_word2 INTO 'result_brute_force' using PigStorage(',');
