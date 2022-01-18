### csv_to_dynamodb_aws_lambda_python_env

# Easy .CSV to DynamoDB

This is a solution to easily and automatically load .csv (comma, tab, etc. delimited) files into AWS-dynamoDB database data tables.

A tool for loading .csv files into DynamoDB should be as simple as "load files" -> "push go" -> "done!" including the following features:
1. It works on a whole folder/directory of files (not just one file at a time).
2. It allows the user to select from_this_row and to_this_row in case only part of a file is to be uploaded (optionally, if desired).
3. It allows for the user to manually specify column data types and column names IF needed, but does not require this by default. 
4. It allows for very large file sizes automatically (with no user action needed). 
5. It should allow for missing data in the .csv files. 
6. It should allow you to create metadata files and inspect csv files for general properties and potential problems. 
7. It should allow split-csv or multiple files to be automatically merged into the same data table.
8. It actively scans for problems with the files and if found reports those to the user. 
9. It will automatically create the data table, the fields, and the data-types, including the primary-key field. 
10. It should be flexibly an (optionally) all-inside-AWS solution, not requiring anyone to have additional special hardware or software, and also allow steps to be done outside of AWS such as analyzing a .csv and the metadata on your local computer.  

### Brief instructions for deployment and use of this tool: 
#### Recombine the zipped uploadable python environment and upload it into an AWS Lambda Function. ( See this guide for splitting and recombining zip archives: https://github.com/lineality/linux_make_split_zip_archive_multiple_small_parts_for_AWS )
Upload your .csv files to AWS-S3 (AWS file storage). Point the tool to the file folder in S3. Hit 'Go' (activate the lambda function). That's all. 

## Overview and Introduction
#### The process of transferring data from a .csv file (for example) into a data table in dynamoDB (an AWS database) (without tools such as this project aims to provide) is not simple. Yet going the other direction, making a .csv from a data table is very simple: just one "make a .csv" button to push. Both should be easy.

## List of properties a .csv file needs to have for it to be ready to be entered into a database table (using this automated system):
1. the name of the .csv file must be formatted to be also the name of the table
2. the first column of the .csv will be the primary key
3. the first column (being the primary key) must be made of unique items, no missing items, and must be all the same data type (e.g. no text strings mixed into otherwise all integers). 
4. the data-types for the columns must be either correctly automatically detectable, or set by you in a simple metadata_file. 

#### In some cases you may want to run separate tests locally on datasets before uploading a modified and cleaned file to AWS. Separate tools for inspecting the above properties are included in this github repo. These datatype-tester tools can be used in google colab, a local python notebook, or a local .py file (e.g. for files too large for colab). 

#### There is a datatype-tester online colab here (and this notebook is in this repo for you to run locally or upload to google colab, aws-sagemaker, etc.):
https://colab.research.google.com/drive/18UwXMKqD-DLBs29RZYHa9RqrVdYa8rvK#scrollTo=_1r1SEvqCfdC 

#### Once this is done, then any number of these checked and clean files should be able to be automatically processed to have the data loaded into database tables in AWS.

# Deploying the python env (venv environment) into AWS:

Making an AWS lambda function is not too difficult but there should be instructions for that...(pending). 
## Brief Instructions
1. Go to aws https://console.aws.amazon.com/
2. Go to lambda functions https://console.aws.amazon.com/lambda/ 
3. Create a function (Press big orange "Create Function" Button)
4. Select: Author from scratch
5. Select: Runtime -> python 3.8 function
6. Add permissions:
- AmazonDynamoDBFullAccess 
- AWSLambdaDynamoDBExecutionRole 
- AWSLambdaInvocation-DynamoDB
- AWSLambdaBasicExecutionRole
- AmazonS3FullAccess 
- AmazonS3ObjectLambdaExecutionRolePolicy 
7. Create the function
8. Upload a zip file containing your function (provides as files in this repo, see below)
9. You can run it like that, or make an api-gateway endpoint (and a colab to use that endpoint...optionally)

You will also need to know how to load files into S3 (file storage in AWS). The S3 web interface is mostly very user friendly. 


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
into dynamoDB.
We start with a set of clean .csv files (single or multiple per table),
we use preexisting or create new metadata files, 
the data.csv and metadata files sorted into a 'completed filed' folder after completion,
and the data is entered into a new (or specified old) dynamoDB database table.


## Example user input:
```
input = {
    directory_name = "YOUR_directory_name"
    s3_bucket_name = "YOUR_BUCKET_NAME"
}
```

## Example metadata_csv file:
```
column_name,AWS_column_dtype,pandas_column_dtype,example_item_list,mixed_datatype_flag_list,missing_data_flag_list,duplicate_data_flag_list
row_number,N,int64,11.0,False,False,False
Years_Experience,N,float64,11.1,False,False,True
Salary,N,int64,39343.0,False,False,False

```



# Rules / Instructions
 
## Cheat Sheet Instruction Summary
1. If you have not yet examined your files (file's metadata) yet,
you can use the tool to make your metadata files (so you can then look at them), by setting the "just_make_metadata_files_flag" to "True":
```
{
 "s3_bucket_name": "YOUR_S3_BUCKET_NAME_HERE",
 "target_directory": "YOUR_FOLDER_NAME_HERE/",
 "just_make_metadata_files_flag": "True"
}
```
 
2. Put .csv files in the S3(AWS) folder.
 
3. Point the lambda function at the correct S3 bucket and your folder:
```
{
 "s3_bucket_name": "YOUR_S3_BUCKET_NAME_HERE",
 "target_directory": "YOUR_FOLDER_NAME_HERE/"
}
```
4. Run the lamabda Function (Hit the "Go" button.)
 
 
 
## Full Instructions
Instruction for using the .csv auto-load Tool:
 
Please read and follow these instructions,
and please report any errors you recieve.
 
 
1. input file(s) must be one or more .csv files (no other formats,
other files will be ignored (or might break the tool))
Do NOT put files into the tool that you do NOT want the tool to process.
 
2. input file(s) (.csv files) must be in a directory in an AWS-S3 folder(directory)
 
3. this tool is an AWS lambda function which is or operates like an api-endpoint
 
4. json-input: the json input for the lambda function(or endpoint),
which directs the tool to your .csv files,
must look like this:
```
{
 "s3_bucket_name": "YOUR_S3_BUCKET_NAME_HERE",
 "target_directory": "YOUR_FOLDER_NAME_HERE/"
}
```
You can also make or use more sub-folders (directories) to organize your files.
In this case combine all folders (the path) to the target directory
```
{
 "s3_bucket_name": "YOUR_S3_BUCKET_NAME_HERE",
 "target_directory": "YOUR_FOLDER_NAME_HERE/YOUR_SUB_FOLDER_NAME_HERE/"
}
```
Here is an example using all optional fields (to be explained below):
```
{
 "s3_bucket_name": "YOUR_BUCKET_NAME",
 "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
 "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/",
 "multi_part_or_split_csv_flag": "True",
 "FROM_here_in_csv": 0,
 "TO_here_in_csv": 4,
 "set_split_threshold_default_is_10k_rows": 5000,
 "just_make_metadata_files_flag": False
}
```
But to not mix levels of directories. S3 is not a real file system, and any sub-folder in the folder that you want will be seen as just more files in that main folder (not files in a subfolder).
 
5. Table Name = File Name:
The csv-tool makes a data-table in an AWS-database from your .csv file (this is the overall goal).
Make the name of your .csv file the same as what you want the AWS database table to be called.
Naming Rules: The name of each file must be:
```
"Between 3 and 255 characters, containing only letters, numbers, underscores (_), hyphens (-), and periods (.)"
```
This is because the database table is given the same name as the .csv file.
Every table must have a unique name (so each .csv file must have a unique name).
 
6. The (database) table must have a unique primary key. And the first column (of your .csv) must be that unique key. A unique row-ID number will work if there is no meaningful unique row key.
 
So you may need to add a unique row. There is a feature in the tool to add a unique row. The tool will exit after adding the row if this option is selected.
e.g.
```
{
 "s3_bucket_name": "YOUR_BUCKET_NAME",
 "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
 "just_make_new_primary_key_first_column": "CSV_FILE_THAT_NEEDS_THE_ROW"
}
```
 
7. The tool will scan for 3 types of primary key errors and give you a warning to fix the file: missing data,
duplicate rows, and
mixed text/number data (e.g. text in a number column).
Finding a warning here halts the whole process, so not all files will have been checked.
 
8. Completed files (fully check and moved into a table) will be moved into
a new directory called (by default) "default_folder_for_completed_csv_files/",
but you can pick a new destination in your endpoint-json if you want:
e.g.
```
"default_folder_for_completed_csv_files" : "THIS_FOLDER/"
```
Files not yet moved into AWS will remain in the original directory.
Please do NOT change the the destination folder to be INSIDE of your sub-folder.
e.g.
```
{
 "s3_bucket_name": "YOUR_BUCKET_NAME",
 "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
 "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/"
}
```
 
9. Please read the output of the function clearly
(to see if there was an error or if the process completed).
Some errors will regard your files and you can fix them.
Other errors may indicate updates needed for the tool.
Please report all errors we can understand this process well.
 
10. Please check the new data-tables in AWS to make sure they look as you want them to look.
 
11. From To:
The default mode is to put one data-csv file into one dynamoDB table,
however you can select from-to for which rows you want to select to upload.
This from-to will disable moving completed files.
From-to cannot be used along with split-multi files.
Starting at 1 or 0 have the same effect, starting from the begining.
```
{
 "s3_bucket_name": "YOUR_BUCKET_NAME",
 "target_directory": "YOUR_FOLDER/OPTIONAL_SUB_FOLDER/",
 "FROM_here_in_csv": 3,
 "TO_here_in_csv": 7
}
```
 
12. Split Files & auto-split:
As with meta-data, file splitting can be automatic or manual. The automatic splitting does not require any addition steps, beyond re-running the function if it times out to keep going through the files.
 
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
 
Note: be careful about mixing split and many other non-split files together, as processing split-files will turn off the protection against over-writing an existing table.
 
Timing Out: The reason why large files must be split is that a lambda function has a maximum time (15min) for how long it can run. If a big .csv file takes more time than 15min, the process will crash in the middle and no progress can be made by re-running the tool. On the other hand, if the S3 folder contains many small files (that each take much less than 15 minutes to run) then running the lambda function over and over will gradually process all of the files. Note that there will still be an error returned when the lambda function times out.
 
13. You will get an error if you try to use split-file and from-to at the same time.
 
14. just_make_metadata_files_flag:
If you want to use the tool to make your file inspection meta_data files and stop there (so you can examine those meta_data files before proceeding), then turn on (set to True) the just_make_metadata_files_flag == True
```
{
 "s3_bucket_name": "YOUR_BUCKET_NAME",
 "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
 "just_make_metadata_files_flag": "False"
}
```







# Workflow (How the Tool Works under the hood)
1. preemptively clear the /tmp/ directory in lambda-function 

2. user inputs a folder (a target s3 directory, in which are .csv files) 

3. scan S3 folder for .csv files (ignore non csv files)

4. make a list of .csv files NOT including "metadata_" at the start. We will later iterate through this file 3 times. 

5. track the many forms of names keeping track of the root name, the lambda name, the old s3 name and the new s3 name, split names, table names, etc.

6. make a list of files that need meta-data files (there are several such related lists, and these are re-created after files are auto split if that happens)  

#### Two steps need to happen before processing the files and iterating through the files to upload them to AWS datatables.
7. There is the option to just create meta-data files before trying to load them. In this case, each data file has a metadata file made which includes primary key type warnings for all columns. 

8. Auto Splitting: In the case of file splitting, the previous file listing steps need to be repeated from scratch and a new list of files made before proceeding. 

#### Iterating Though and Processing Files
9. iteration 1: iterate through list of data-csv files and check to see if there are any name collisions between file names and dynamoDB tables to be created (exit to error message if collision found) Note: extra logic for where from-to or multiple input files are used.

10. iteration 2 and auto-split: iterate through files_that_need_metadata_files_list of data-csv files to make a list of unpaired files: use pandas to create a table with AWS datatypes, and move that metadata_file to s3. The goals here is to allow users to upload custom metadata files, but to default to automating that process.  
Plus auto-split
check each data csv file's shape to see if the file has more than 10,000 rows. If so: split/replace the file in S3.

11. iteration 3: when all data-csv files are paired with a metadata_ file:  iterate through the list of all root files (see below)

#### The next steps are done for each (iterating through each) data file in the 3rd and last pass through the list of data-csv files:

12. The primary-key column/field is error-checked for 3 types of primary key errors and gives the user a warning to fix the file: missing data, duplicate rows, and mixed text/number data (e.g. text in a number column). Finding and outputting a warning here halts the whole process, so not all files will have been checked. 

13. lambda creates a new dynamoDB table with a name the same as the .csv file. Note: extra logic to skip this for from-to or multi-file-to-one-table input.

14. lambda uses metadata_ file and data-csv file to load data into dynamoDB. Note: by default this is the whole file, but optionally a from_row and to_row can be specified by row number in csv

15. after file is loaded successfully into dynamoDB, data-csv and metadata_csv are moved to a new directory (folder) called 'transferred files' (or whatever the name ends up being) (this involves copying to the new location and then deleting the old file from S3). Note: this is skipped when using from-to or multi-file-to-one-table as the whole upload process is not completed in one step.

16. the aws Lambda /tmp/ copy of the file is deleted (to not overwhelm the fragile lambda-function)

#### These steps are done at the very end of the whole process after all files are processed (if there is no error that stops the process)
17. remove all files from lambda /tmp/ directory

18. output: list of tables created OR error message




# More Notes:
DynamoDB does include more data-types which can at least be manually set in a metadata file, and perhaps added into the automated detector. 
e.g.
```
String (text) = S
Number (short, long, int, float etc) = N
Boolean = BOOL
Map (dictionary/json) = M
List (array) = L

```


# More Commentary

The ideal is to make this tool as simple to use as possible. But it is not always ideal for the user to do literally nothing and not even look at or make any decisions about what they are doing. Yes, this tool is designed to simply try to process whatever file you put into it, but that is based on the assumption that the user has chosen and crafted that file to be processed. 


