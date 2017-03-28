/* 
  1. Read a file, flatten lines as words and remove all puncutaion and convert to lower-case 
  2. calculate the number of words('c') and log c
  3. Foreach word, calculate the frequency of its occurance('a')
  4. Emit the following information for record
	word, a, c, a(log a - log c)
*/
-- Setting the name of the job to debug in the applications
SET job.name 'pre_process_stage1';

-- Loading the fule using TextLoader as our data is in UTF-8 format
read_file = LOAD '$raw_input_file'  USING TextLoader as (line:chararray);

-- Seperate words, put them one on each line, convert to lower case, replace all punctuations and control characters to a white space, trim all white spaces
flatten_words = FOREACH read_file GENERATE FLATTEN(TOKENIZE(REPLACE(LOWER(TRIM(line)),'[\\p{Punct},\\p{Cntrl}]','')));

-- Group all lines to count the total number of words in this document
group_all_count = group flatten_words all;

-- get the count of all words
count_all_words = FOREACH group_all_count GENERATE COUNT(flatten_words);
-- DUMP count_all_words;

-- Calculate the Log (base-e) of total number of words. This is a one time calculation to be used in the calculations
log_c = FOREACH count_all_words GENERATE LOG($0);
-- DUMP log_c;

-- group by word to count frequency
grp_by_word = GROUP flatten_words by $0;

-- Calculate the frequency of each word
freq = FOREACH grp_by_word GENERATE $0, COUNT($1), count_all_words.$0,  COUNT($1)*(LOG(COUNT($1)) - log_c.$0);
STORE freq into '$pre_processed_file' using PigStorage(',');
