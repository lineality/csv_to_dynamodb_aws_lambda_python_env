### csv_to_dynamodb_aws_lambda_python_env

# Easy .CSV to DynamoDB

This is a solution to easily and automatically load .csv (comma, tab, etc. delimited) files into AWS-dynamoDB database data tables.

Overall, a tool for loading .csv files into DynamoDB should be as simple as "load files" -> "push go" -> "done!" including the following features:
1. It works on a whole folder/directory of files (not just one file at a time).
2. It allows the user to select from_this_row and to_this_row in case only part of a file is to be uploaded (optionally, if desired).
3. It allows for the user to manually specify column data types and column names IF needed, but does not require this by default. 
4. It allows for very large file sizes automatically (with no user action needed). 
5. It should allow for missing data in the .csv files. 
6. It should allow split-csv or multiple files to be automatically merged into the same data table.
7. It should be an all-inside-AWS solution, not requiring anyone to have special hardware or software. 
8. It actively scans for problems with the files and if found reports those to the user. 
9. It will automatically create the data table, the fields, and the data-types, including the primary-key field. 

#### Instructions in brief: Recombine the zipped uploadable python environment and upload it into an AWS Lambda Function. Upload your .csv files to AWS-S3. Hit 'Go' (activate the lambda function). That's all. 

## Overview and Introduction
#### The process of transferring data from a .csv file (for example) into a data table in dynamoDB (an AWS database) is not simple. Going the other way, making a .csv from a table is very simple, just one "make a .csv" button to push. Both should be easy.

## List of properties a .csv file needs to have for it to be ready to be entered into a database table (using this automated system):
1. the name of the .csv file must be formatted to be also the name of the table
2. the first column of the .csv will be the primary key
3. the first column (being the primary key) must be made of unique items, no missing items, and must be all the same data type (e.g. no text strings mixed into otherwise all integers). 
4. the data-types for the columns must be either correctly automatically detectable, or set by you in a simple metadata_file. 

#### Tools for a simple inspection of the above properties are included in this github repo. These datatype-tester tools can be used in google colab, a local python notebook, or a local .py file (e.g. for files too large for colab). 

And there's a datatype-tester online colab here (and this notebook is in this repo for you to run locally or upload to google colab, aws-sagemaker, etc.):
https://colab.research.google.com/drive/18UwXMKqD-DLBs29RZYHa9RqrVdYa8rvK#scrollTo=_1r1SEvqCfdC

#### Once this is done, then any number of these checked and clean files should be able to be automatically processed to have the data loaded into database tables in AWS.

# Deploying the python ENV into AWS:

Making an AWS lambda function is not too difficult but there should be instructions for that...(pending). 
## Roughly:
1. go to aws
2. go to lambda functions
3. create a function
4. select that it be a python 3.8 function
5. add some permissions (see details below)
6. create the function
7. upload a zip file containing your function (provides as files in this repo, see below)
8. you can run it like that, or make an api-gateway endpoint (and a colab to use that endpoint...optionally)

You will also need to know how to load files into S3 (file storage in AWS).

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

# Rules / Instructions
Instruction for using the .csv auto-load Tool:

Please read and follow these instructions, and please tell me about any errors you recieve. 

1. input file(s) must be one or more .csv files (no other formats)

2. input file(s) must be in a directory in an AWS S3 folder(directory)

3. this tool is an AWS lambda function which is or operates like an api-endpoint

4. json-input: the json input for the lambda function(or endpoint)
must look like this:
```
{
  "S3_BUCKET_NAME": "YOUR_S3_BUCKET_NAME_HERE",
  "target_directory": "YOUR_FOLDER_NAME_HERE/"
}
```
You can also make or use more sub-folders (directories) to organize your files. In this case combine all folders (the path) to the target directory
```
{
  "S3_BUCKET_NAME": "YOUR_S3_BUCKET_NAME_HERE",
  "target_directory": "YOUR_FOLDER_NAME_HERE/YOUR_SUB_FOLDER_NAME_HERE/"
}
```
Here is an example using all optional fields (to be explained below):
```
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
  "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/",
  "multi_part_or_split_csv_flag": "True",
  "FROM_here_in_csv": 0,
  "TO_here_in_csv": 4,
  "set_split_threshold_default_is_10k_rows": 5000
}
```

5. The csv-tool makes a data-table in an AWS-database from your .csv file. Make the name of your .csv file the same as what you want the AWS database table to be called. The name of each file must be:
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
e.g.
```
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
  "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/"
}
```

9. Please read the output of the function clearly (to see if there was an error or if the process completed). Some errors will regard your files and you can fix them. Other errors may indicate updates needed for the tool. Please report all errors we can understand this process well. 

10. Please check the new data-tables in AWS to make sure they look as you want them to look. 

11. From To: The default mode is to put one data-csv file into one dynamoDB table, however you can select from-to for which rows you want to select to upload. This from-to will disable moving completed files. From-to cannot be used along with split-multi files. Starting at 1 or 0 have the same effect, starting from the begining. 
```
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_FOLDER/OPTIONAL_SUB_FOLDER/",
  "FROM_here_in_csv": 3,
  "TO_here_in_csv": 7
}
```

12. Split Files & auto-split: As with meta-data, file splitting can be automatic or manual. The automatic splitting does not require any addition steps, beyond re-running the function if it times out to keep going through the files. 

Sometimes csv files are very large and it is best to split them into pieces before uploading them (so that the tool does not time-out in the middle of a file). 
This csv uploader tool is designed to work with (and includes a version of)  this csv splitter:
https://github.com/lineality/split_csv_python_script 

As a rule of thumb
files with more than 10,000 row should be split. The auto-splitter will do this automatically, though you custom set threshold, which is default set to 10,000 rows. this json choice is called "set_split_threshold_default_is_10k_rows"

If there are many parts and the tool times out, just keep running it until all the parts get completed and moved to the 'completed files" folder.

You can manually split the file yourself and put all the split files into the target direction, 
hit GO (proverbially) and the tool will put them all into the same table.
Each part must be suffixed with _split__###.csv 

 This function also works to put two data-csv files into the SAME table BUT: be careful not to overwrite an existing table, and multiple component files (when putting multiple data-csv files into one dynamoDB table) must be given the same name and individually put into S3 and run separately.

Note: be careful about mixing split and many other non-split files together, as processing split-files will turn off the protection against overwriting an existing table. 

13. You will get an error if you try to use split-file and from-to at the same time.



# Workflow (How the Tool Works under the hood)
1. pre-emptively clear the /tmp/ directory in lambda-function 
2. user inputs a folder (a target s3 directory, in which are .csv files) 
3. scan S3 folder for .csv files 
4. make a list of .csv files NOT including "metadata_" at the start. We will later iterate through this file 3 times. 
5. track the many forms of names keeping track of the root name, the lambda name, the old s3 name and the new s3 name, split names, table names, etc.
6. make a list of files that need meta-data files (there are several such related lists, and these are re-created after files are auto split if that happens)  
7. iteration 1: iterate through list of data-csv files and check to see if there are any name collisions between file names and dynamoDB tables to be created (exit to error message if collision found) Note: extra logic for where from-to or multiple input files are used.
8. iteration 2 and auto-split: iterate through files_that_need_metadata_files_list of data-csv files to make a list of unpaired files: use pandas to create a table with AWS datatypes, and move that metadata_file to s3. The goals here is to allow users to upload custom metadata files, but to default to automating that process.  
Plus auto-split
check each data csv file's shape to see if the file has more than 10,000 rows. If so: split/replace the file in S3.

9. iteration 3: when all data-csv files are paired with a metadata_ file:  iterate through the list of all root files (see below)

#### The next steps are done for each (iterating through each) data file in the 3rd and last pass through the list of data-csv files:

10. The primary-key column/field is error-checked for 3 types of primary key errors and gives the user a warning to fix the file: missing data, duplicate rows, and mixed text/number data (e.g. text in a number column). Finding and outputting a warning here halts the whole process, so not all files will have been checked. 

11. lambda creates a new dynamoDB table with a name the same as the .csv file. Note: extra logic to skip this for from-to or multi-file-to-one-table input.
12. lambda uses metadata_ file and data-csv file to load data into dynamoDB. Note: by default this is the whole file, but optionally a from_row and to_row can be specified by row number in csv
13. after file is loaded successfully into dynamoDB, data-csv and metadata_csv are moved to a new directory (folder) called 'transferred files' (or whatever the name ends up being) (this involves copying to the new location and then deleting the old file from S3). Note: this is skipped when using from-to or multi-file-to-one-table as the whole upload process is not completed in one step.
14. the aws Lambda /tmp/ copy of the file is deleted (to not overwhelm the fragile lambda-function)

#### These steps are done at the very end of the whole process after all files are processed (if there is no error that stops the process)
15. remove all files from lambda /tmp/ directory
16. output: list of tables created OR error message




