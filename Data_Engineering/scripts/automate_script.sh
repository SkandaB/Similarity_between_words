#!/bin/sh

# This script will automate the work for finding similarity between words across documents
#
# Takes 4 parameters as input
# $1 = name of file which contains the list of files(books) to be downloaded and used for similarity
# $2 = HDFS directory to store raw files
# $3 = HDFS directory to store the pre-processed files for each document
# $4 = HDFS directory to store final results of pair_wise documents

# Create a temp directory in local filesystem to download books from web
temp_working_dir="temp_dir_"$(date +%s)
mkdir "$temp_working_dir"

cd $temp_working_dir
hadoop_destination=$2
hadoop_pre_processed=$3
hadoop_final_result=$4

# If input file is not present or if file is empty, exit
if [[ ! -f $1 ]] -o [[! -s $1]]; then
    echo 'Input file does not exist, Aborting ...'
    exit
fi

file_names=()
# Read the web_links for books from a given text file, Stage in local machine and upload to HDFS, Pre-process it in Pig. Clean-up the temporary directory
while IFS='' read -r line || [[ -n "$line" ]]; do
    wget --quiet $line
    latest_file="$(ls -t|head -n1)"
    file_names+=("$latest_file")
    hadoop fs -put $latest_file "$hadoop_destination"
    pig -f pre_process.pig -param raw_input_file="$hadoop_destination/$latest_file" -param pre_processed_file="$hadoop_pre_processed/$latest_file"
    rm $latest_file
done < "../$1"


# Get nC2 pair-wise combinations from the array, call Pig scipt and use these two documents as input
a_length=${#file_names[@]}
for ((i=0; i <${a_length}; i++)); do
  for ((j=$i+1; j<${a_length}; j++)); do
    final_result="$hadoop_final_result/${file_names[$i]}_${file_names[$j]}"
    file1="$hadoop_pre_processed/${file_names[$i]}"
    file2="$hadoop_pre_processed/${file_names[$j]}"
    echo "pig -f join_docs.pig -param pre_processed_file1=$file1 -param pre_processed_file2=$file2 -param sim_result=$final_result"
    pig -f join_docs.pig -param pre_processed_file1=${file_names[$i]} -param pre_processed_file2=${file_names[$j]} -param sim_result=$final_result
  done
done
cd ..

# Clean-up this temp local directory at the end of script
rm -r $temp_working_dir