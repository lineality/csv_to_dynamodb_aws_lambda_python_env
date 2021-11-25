# env_aws_lambda_auto_load_csv_to_dynamodb

This is an AWS Lambda Function (more specifically, a zipped uploadable python environment file for an AWS Lambda Function) that will automatically load a cleaned .csv file into a table (which the tool creates) in AWS dynamoDB (a NoSQL database).

## Overview and Introduction
The process of transferring data from a .csv file (for example) into a data table in dynamoDB (an AWS database) is not simple. Going the other way, making a .csv from a table is very simple, just one "make a .csv" button to push. 

#### The goal here has been to automate and streamline the process of entering a .csv file into AWS (into a dynamoDB table) to make it as effortless as possible. Ideally: put the .cvs files into a folder, push the proverbial or literal 'Start' button, and that's it. 

#### Yes, you cannot take any old random file and expect it to go cleanly into an SQL or NoSQL database. But I contend that it is very reasonable to say that even a non-technically-inclined person can check a .csv file to see it has the appropriate properties.

# List of properties a .csv file need to have for it to be ready to be entered into a database table (using this automated system):
1. the name of the .csv file must be formatted to be also the name of the table
2. the first column of the .csv will be the primary key
3. the first column (being the primary key) must be made of unique items, no missing items, and must be all the same data type (e.g. no text strings mixed into otherwise all integers). 
4. the data-types for the columns must be either correctly automatically detectable, or set by you in a simple metadata_file. 

#### Tools for a simple inspection of the above properties are included in this github repo. These datatype-tester tools can be used either for google colab, a local python notebook, or a local .py file. 

#### Once this is done, then any number of these checked and clean files should be able to be automatically loaded into AWS. 


# Deploying the python ENV into AWS:

#### The env file is split into small parts for easier uploading and downloading.
Follow these instructions 
```
https://github.com/lineality/linux_make_split_zip_archive_multiple_small_parts_for_AWS
```
to recombine the zip archive into one piece so that it can be uploaded (not unzipped) into AWS.


You will need to create an AWS-Lambda-Function with these permissions (given to the AWS-Lambda-Function):
```
    AmazonDynamoDBFullAccess 
    AWSLambdaDynamoDBExecutionRole 
    AWSLambdaInvocation-DynamoDB

    AWSLambdaBasicExecutionRole

    AmazonS3FullAccess 
    AmazonS3ObjectLambdaExecutionRolePolicy 
```

The idea is to dynamically automate the process of moving .csv files
into dynamoDB,
starting with a set of clean .csv files
and ending with metadata files, file sorted into a 'completed' folder,
and data entered into a new dynamoDB table


### Example input
```
input = {
    directory_name = "YOUR_directory_name"
    S3_BUCKET_NAME = "YOUR_BUCKET_NAME"
}
or
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_directory_name/SUB_FOLDER_OPTIONAL/"
  "default_folder_for_completed_csv_files": "YOUR_FOLDER_NAME/"
}
```

# example metadata_csv file:
```
headers, dynamoBD_data_type, 
item_id, S
item_number, N
```


## Rules / Instructions
Instruction for using the Tool:

Please read and follow these instructions, and please tell me about any errors you recieve. 

## Rules / Instructions
1. file input must be one or more .csv files (no other formats)

2. file input must be in a directory in S3

3. the tool is an AWS lambda function which is or operates like an api-endpoint

4. directories & api-input: the json input for the lambda function(or endpoint)
must look like this
{
  "S3_BUCKET_NAME": "YOUR_S3_BUCKET_NAME_HERE",
  "target_directory": "YOUR_FOLDER_NAME_HERE/"
}

You can also make or use more sub-folders (directories) to organize your files. In this case combine all folders (the path) to the target directory

{
  "S3_BUCKET_NAME": "YOUR_S3_BUCKET_NAME_HERE",
  "target_directory": "YOUR_FOLDER_NAME_HERE/YOUR_SUB_FOLDER_NAME_HERE/"
}


5. The csv-tool make a data-table in an AWS-database from your .csv file. Make the name of your .csv file the same as what you want the AWS database table to be called. The name of each file must be:
```
Between 3 and 255 characters, containing only letters, numbers, underscores (_), hyphens (-), and periods (.)
```
This is because the database table is given the same name as the .csv file.
Every table must have a unique name (so each .csv file must have a unique name). 

6. The table must have a unique primary key. And the first column must be that unique key. A unique row-ID number will work if there is no meaningful unique row key. (So you may need to add that (ask if you need help).

7. The tool will scan for 3 types of primary key errors and give you a warning to fix the file: missing data, 
duplicate rows, and 
mixed text/number data (e.g. text in a number column). 
Finding a warning here halts the whole process, so not all files will have been checked. 

8. Completed files (fully check and moved into a table) will be moved into a new directory called (by default) "default_folder_for_completed_csv_files/", but you can pick a new destination in your endpoint-json if you want: 
e.g.
"default_folder_for_completed_csv_files" : "THIS_FOLDER/"
Files not yet moved into AWS will remain in the original directory. 
Please do NOT change the the destination folder to be INSIDE of your sub-folder.

9. Please read the output of the function clearly (to see if there was an error or if the process completed). Some errors will regard your files and you can fix them. Other errors may indicate updates needed for the tool. Please report all errors we can understand this new process well. 

10. Please check the new data-tables in AWS to make sure they look as you want them to look. 

## workflow
1. pre-emptively clear the /tmp/ directory in lambda-function (OK)
2. input a folder (a target s3 directory, in which are .csv files) (OK)
3. scan folder for .csv files (OK)
4. make a list of .csv files NOT including "metadata_" at the start. We will later iterate through this file 3 times. (OK)
5. make a system to track form of names keeping track of the root name, the lambda name, the olds3 name and the news3 name is task
6. make a list of files that need meta-data files (OK) 
7. iteration 1: iterate through list of data-csv files and check to see if there are any name collisions between file names and dynamoDB tables to be created (exit to error message if collision found) (OK)
8. iteration 2: iterate through files_that_need_metadata_files_list of data-csv files to make a list of unpaired files: use pandas to create a table with AWS datatypes, and move that metadata_file to s3 (ok)
9. iteration 3: when all data-csv files are paired with a metadata_ file:  iterate through the list of all root files (see below)

#### The next steps are done for each (iterating through each) data file in the 3rd and last iteration through the list of data-csv files:

10. The primary-key column/field is error-checked for 3 types of primary key errors and give you a warning to fix the file: missing data, duplicate rows, and mixed text/number data (e.g. text in a number column). Finding a warning here halts the whole process, so not all files will have been checked. 
11. lambda creates a new dynamoDB table with a name the same as the .csv file
12. lambda uses metadata_ file and data-csv file to load data into dynamoDB
13. after file is moved, data-csv and metadata_csv are moved to a new directory (folder) called 'transferred files' (or whatever the name ends up being) (this involves copying to the new location and then deleting the old file from S3)
14. the aws /tmp/ copy of th file is deleted (to not overwhelm the fragile tiny lambda-function)

#### These steps are done at the very end of the whole process after all files are processed (if there is no error that stops the process)
15. remove all files from lambda /tmp/ directory
16. output: list of tables created OR error message

