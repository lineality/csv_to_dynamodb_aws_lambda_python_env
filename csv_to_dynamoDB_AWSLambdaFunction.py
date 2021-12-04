"""
v11
2021.12.04
dynamic .csv to AWS dynamoDB - AWS-lambda-function

lambda function will need these permissions:
    AmazonDynamoDBFullAccess 
    AWSLambdaDynamoDBExecutionRole 
    AWSLambdaInvocation-DynamoDB

    AWSLambdaBasicExecutionRole

    AmazonS3FullAccess 
    AmazonS3ObjectLambdaExecutionRolePolicy 


The idea is to dynamically automate the process of moving .csv files
into dynamoDB,
starting with a set of clean .csv files
and ending with metadata files, file sorted into a 'completed' folder,
and data entered into a new dynamoDB table

TODO:
1. another version to just make metadata files to inspect
2. another version to add data to another existing dynamoDB table
maybe: merging .csv files?
3. should directory be followed by a / in input?

# Example input
input = {
    directory_name = "YOUR_S3_DIRECTORY_NAME",
    S3_BUCKET_NAME = "YOUR_BUCKET_NAME"
}

e.g.
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME"
}
or
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
  "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/"
}
or: if you are going from_to within an input file
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
  "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/",
  "multi_part_or_split_csv_flag": "True"
}
or: if you are going from_to within an input file
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME",
  "default_folder_for_completed_csv_files": "COMPLETED_FILES_FOLDER_NAME/",
  "multi_part_or_split_csv_flag": "True",
  "FROM_here_in_csv": 0,
  "TO_here_in_csv": 4
}

{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_S3_DIRECTORY_NAME/OPTIONAL_SUBFOLDER_NAME/",
  "multi_part_or_split_csv_flag": "True",
  "FROM_here_in_csv": 0,
  "TO_here_in_csv": 4
}

https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.DataTypes.html
https://www.tutorialspoint.com/dynamodb/dynamodb_data_types.htm
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes

# example metadata_csv file, for making custom files:
```
headers, dynamoBD_data_type, 
item_id, S
item_number, N
```
"""



"""
# Rules / Instructions
Instruction for using the .csv auto-load Tool:

Please read and follow these instructions, and please tell me about any errors you recieve. 

1. file input must be one or more .csv files (no other formats)

2. file input must be in a directory in S3

3. the tool is an AWS lambda function which is or operates like an api-endpoint

4. directories & api-input: the json input for the lambda function(or endpoint)
must look like this
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
  "TO_here_in_csv": 4
}
```

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

11. The default mode is to put one data-csv file into one dynamoDB table, however you can select from-to for which rows you want to select to upload. This function also works to put two data-csv files into the SAME table BUT: be careful not to overwrite an existing table, and multiple component files (when putting multiple data-csv files into one dynamoDB table) must be given the same name and individually put into S3 and run separately. (Note: separate functionality could be made to combine many small files into one table but usually this is used when csv files are too BIG to load in all at one time. Not being able to load the table all at once -> you can now load the table in separate batches. You need to set a multfile flag and (optionally) select from and to with your inputs. Starting at 1 or 0 have the same effect, starting from the begining. 
```
{
  "S3_BUCKET_NAME": "YOUR_BUCKET_NAME",
  "target_directory": "YOUR_FOLDER/OPTIONAL_SUB_FOLDER/",
  "multi_part_or_split_csv_flag": "True",
  "FROM_here_in_csv": 3,
  "TO_here_in_csv": 7
}
```


# Workflow
1. pre-emptively clear the /tmp/ directory in lambda-function 
2. user inputs a folder (a target s3 directory, in which are .csv files) 
3. scan S3 folder for .csv files 
4. make a list of .csv files NOT including "metadata_" at the start. We will later iterate through this file 3 times. 
5. track forms of names keeping track of the root name, the lambda name, the olds3 name and the news3 name is task, etc.
6. make a list of files that need meta-data files  
7. iteration 1: iterate through list of data-csv files and check to see if there are any name collisions between file names and dynamoDB tables to be created (exit to error message if collision found) Note: extra logic for where from-to or multiple input files are used.
8. iteration 2: iterate through files_that_need_metadata_files_list of data-csv files to make a list of unpaired files: use pandas to create a table with AWS datatypes, and move that metadata_file to s3. The goals here is to allow users to upload custom metadata files, but to default to automating that process.  
9. iteration 3: when all data-csv files are paired with a metadata_ file:  iterate through the list of all root files (see below)

#### The next steps are done for each (iterating through each) data file in the 3rd and last pass through the list of data-csv files:

10. The primarny-key column/field is error-checked for 3 types of primary key errors and gives the user a warning to fix the file: missing data, duplicate rows, and mixed text/number data (e.g. text in a number column). Finding and outputting a warning here halts the whole process, so not all files will have been checked. 

11. lambda creates a new dynamoDB table with a name the same as the .csv file. Note: extra logic to skip this for from-to or multi-file-to-one-table input.
12. lambda uses metadata_ file and data-csv file to load data into dynamoDB. Note: by default this is the whole file, but optionally a from_row and to_row can be specified by row number in csv
13. after file is loaded successfully into dynamoDB, data-csv and metadata_csv are moved to a new directory (folder) called 'tranferred files' (or whatever the name ends up being) (this involves copying to the new location and then deleting the old file from S3). Note: this is skipped when using from-to or multi-file-to-one-table as the whole upload process is not completed in one step.
14. the aws Lambda /tmp/ copy of the file is deleted (to not overwhelm the fragile lambda-function)

#### These steps are done at the very end of the whole process after all files are processed (if there is no error that stops the process)
15. remove all files from lambda /tmp/ directory
16. output: list of tables created OR error message
"""

# Import Libraries and Packages for Python
import boto3
import glob 
import json
import io
import os
import re
import pandas as pd
import time
import datetime


###################
# Helper Functions
###################


# helper function
# def get_S3_subdirectories_list(s3_client, s3_bucket, target_directory):
#     """
#     Make a crude list of file contents that needs to be further processed later

#     This version does NOT include files in sub-directories
#     """
#     # get list of files in SUB-directories
#     sub_directory_list = []

#     result = s3_client.list_objects(Bucket=s3_bucket, Prefix=target_directory, Delimiter='/')

#     prefix = 'prefix-name-with-slash/'  
    
#     for o in result.get('CommonPrefixes'):
#         sub_item = o.get('Prefix')
#         # print("sub-item", sub_item)
#         sub_directory_list.append( sub_item )

#     # return the list of file names from the AWS S3 directory
#     return sub_directory_list


# # helper function
# def remove_subdirectory_files_from_list(list_all, list_of_subdirectories):

#     output_list = []

#     # iterate through list of all items
#     for this_item in list_all:

#         # inspection
#         print("this item:", this_item)

#         flag = False

#         # if str(obj) not in a subfolder
#         for sub_item in list_of_subdirectories:

#             # inspection
#             print("sub item:", sub_item)
            
#             print("regex_boolean_detect_pattern( this_item, sub_item )", regex_boolean_detect_pattern( this_item, sub_item ))

#             # if sub-directory found in the item-list (remove that item)
#             if regex_boolean_detect_pattern( this_item, sub_item ) == True:
#                 flag = True

#         print("flag", flag)

#         if flag == False:
#             # make a list of just stripped-down file names

#             #inspection
#             print("append")

#             output_list.append( this_item )

#             # inspection
#             print(output_list)

#     return output_list  

# Helper Function
def remove_split_from_name(name):
    """
    if last part of name is _split__###
    return: name - "split"

    Else, return: name
    """
    if name[-15:-7] == "_split__":
        return name[:-15] + ".csv"
    else:
        return name


# Helper Function
def make_primary_key_warning_flag_list(df):

    #############
    # Make Flags
    #############

    mixed_datatype_flag = False
    missing_data_flag = False
    duplicate_data_flag = False

    ############################
    # check mixed_datatype_flag
    ############################

    if str(df.iloc[:, 0].dtype) == 'object':
        mixed_datatype_flag = True

    # # for terminal or inspection
    # print( mixed_datatype_flag )

    ############################
    # check missing_data_flag
    ############################

    # check is na
    if df.iloc[:, 0].isna().sum() != 0:
        missing_data_flag = True

    # check is null
    if df.iloc[:, 0].isnull().sum() != 0:
        missing_data_flag = True

    # # for terminal or inspection
    # print( missing_data_flag )

    ############################
    # check duplicate_data_flag
    ############################

    if ( df.iloc[:, 0].value_counts().sum() == len(df.iloc[:, 0].value_counts()) ) == False:
        duplicate_data_flag = True

    # # for terminal or inspection
    # print( duplicate_data_flag )

    #####################
    # Make list of flags
    #####################

    warning_flag_list = []

    if mixed_datatype_flag == True:
        warning_flag_list.append( 'mixed_datatype_flag' )
    if missing_data_flag == True:
        warning_flag_list.append( 'missing_data_flag' )
    if duplicate_data_flag == True:
        warning_flag_list.append( 'duplicate_data_flag' )

    ###############################
    # return list of warning flags
    ###############################

    return warning_flag_list

# Helper function
def regex_boolean_detect_pattern(text_item, pattern):
    """
    requires: import re

    This helps with detecting the ready-state of a new/being made 
    dynamoDB table, checking to see if the status = "creating"
    """
    # run regex to search for the pattern in the text 
    results = re.findall(pattern, text_item)

    # return a boolean for True if the text is found
    return len(results) == 1


# Helper function
def regex_boolean_detect_creating(text_item):
    """
    This helps with detecting the ready-state of a new/being made 
    dynamoDB table, checking to see if the status = "creating"
    """

    # look for the pattern of: tablestatus = creating
    pattern = r"'TableStatus': 'CREATING'"

    # run regex to search for the pattern in the text 
    results = re.findall(pattern, text_item)

    # return a boolean for True if the text is found
    return len(results) == 1


# Helper Function
def make_table_in_aws_dynamoDB (dynamoDB_client, table_name, primary_key_name, key_dtype):
    """
    Get from metadata the name and dtype (data type) of the first column.
    """
    # Connect to ...
    table = dynamoDB_client.create_table(
        TableName = table_name,
        KeySchema=[
            {
                'AttributeName': primary_key_name,
                'KeyType': 'HASH'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': primary_key_name,
                'AttributeType': key_dtype
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    # print for terminal
    print(f"Table Made!! {table_name}")
    print(f"New Table Made in AWS-dynamoDB. Table name: {table_name}, key: {primary_key_name}, key dtype {key_dtype}")

    return True

# Helper Function
def get_primary_key_dtype_data_to_make_table(metadata_df):
    """
    example use:
      primary_key_name, key_dtype = get_primary_key_dtype_data_to_make_table(meta_df)
    """

    primary_key_name = metadata_df.iloc[0][0]
    key_dtype = metadata_df.iloc[0][1]

    return primary_key_name, key_dtype


# helper function to move files within AWS S3
def move_file_in_S3(s3_resource, S3_bucket_target, target_directory, target_file_key, default_folder_for_completed_csv_files):
    # combine to get your full pathway to file
    target_file_and_directory = f"{target_directory}{target_file_key}"
    
    # set your variables
    original_key = target_file_and_directory
    destination_key = default_folder_for_completed_csv_files + target_file_key
    
    original_file_data = {
        'Bucket': S3_bucket_target,
        'Key': original_key
    }
    
    # COPY the file (so then in two places, two copies)
    s3_resource.meta.client.copy(original_file_data, S3_bucket_target, destination_key)

    # Delete the former item (leaving only the moved item)
    s3_resource.Object(S3_bucket_target, original_key).delete()

    return None


# helper function to ulpload a file to AWS S3
def upload_file_to_S3(s3_resource, S3_bucket_target, python_tmp_file_location, new_S3_file_directory_and_name_key):

    s3_resource.Bucket(S3_bucket_target).upload_file(python_tmp_file_location, new_S3_file_directory_and_name_key)

    return None


# helper function to clear remaining .csv files from /tmp/ directory
def clear_tmp_directory():

    """
    requires:
        import os (to remove file)
        import glob (to get file list)
    """

    # use glob to get a list of remaining .csv files
    remaining_files_list = glob.glob("/tmp/*.csv")
      
    # File location
    location = "/tmp/"

    # iterate through list of remaining .csv files
    for this_file in remaining_files_list: 
        # Remove this_file
        os.remove(this_file)

    # AGAIN use glob to get a list of remaining .csv files
    remaining_files_list = glob.glob("/tmp/*.csv")

    return print("""/tmp/ cleared. Check that directory is empty. remaining_files_list = """, remaining_files_list )

# helper function 
def print_aws_tmp_files():

    """
    requires:
        import os (to remove file)
        import glob (to get file list)
    """

    # use glob to get a list of remaining .csv files
    aws_tmp_files_list = glob.glob("/tmp/*")
      
    return print("/tmp/ files_list = ", aws_tmp_files_list )

# helper function 
def make_metadata_csv(name_of_csv):
    """
    This function makes a metadata_ file
    with three kinds of data:
    1. the pandas datatype for each column
    2. the AWS dynamoDB data type for each column
    3. an example of the data for each column

    Requires: pandas as pd
    """

    # load file into pandas dataframe
    df = pd.read_csv( name_of_csv )

    # extract list of column names from .csv
    column_name_list = list(df.columns)

    # # inspection
    # print(column_name_list)

    # make empty list for datatypes, the same size as the name-list
    pandas_dtypes_list = [None]*len(column_name_list)
    AWS_dtypes_list = [None]*len(column_name_list)

    # make a list of example items for inspection
    example_item_list = list(df.loc[0])

    # extract the datatype of each column as recognized by pandas
    for index, column_name in enumerate(column_name_list):
        pandas_dtypes_list[index] = str(df[column_name].dtypes)

    # # inspection
    # print(pandas_dtypes_list)

    # conversion_dictionary
    conversion_dict = {
        "object" : 'S',
        "int64" : 'N',
        "float64" : 'N',
        "datetime64" : 'S',
        "bool" : 'BOOL',
    }

    # convert to AWS-DynamoDB datatypes
    # look up each item in converstion dictionary
    # and put AWS value in new list
    for index, column_name in enumerate(pandas_dtypes_list):
        AWS_dtypes_list[index] = conversion_dict[column_name]

    # make a dictionary of lists 
    type_dict = {'column_name': column_name_list, 
                 'AWS_column_dtype': AWS_dtypes_list, 
                 'pandas_column_dtype': pandas_dtypes_list,
                 'example_item_list': example_item_list,
                 } 
        
    # make a new pandas dataframe based on the dictionary of lists    
    df_meta = pd.DataFrame(type_dict)
        
    # make file name for csv meta_data file (for AWS or for normal OS)
    # slice to remove the first /tmp/ name
    new_name_of_csv = "/tmp/metadata_" + name_of_csv[5:]

    # # for normal OS or python notebook
    # new_name_of_csv = "metadata_" + name_of_csv

    """
    Below are two different versions of formatted output
    in terms of the structure
    of the resulting .csv file
    """

    # # saving the dataframe
    # df_meta.to_csv( new_name_of_csv )

    # # saving the dataframe (alternate version)
    df_meta.to_csv( new_name_of_csv , header=True, index=False)

    # delete dataframes to save memory
    del df
    del df_meta

    # end program
    return None


# helper function
def make_dynamo_item(dataframe, metadata_dictionary, row_number):

    Item = {}

    for column_index in metadata_dictionary:
        Item[ metadata_dictionary[column_index]['column_name'] ] = { metadata_dictionary[column_index]['AWS_column_dtype'] : str( dataframe.iloc[row_number][column_index] ) }

    return Item



# helper function
def get_file_name(text_input):
    """
    Strips target file name out of padding
    """
    # 'after this' choice of characters
    after_this = 'key='

    # get item after your choice of characters:
    pattern = f'(?<={after_this}).*$'
    
    # use regex
    target_file_name = re.findall(pattern,text_input) 

    # strip out extra characters with a string-slice
    target_file_name = str(target_file_name[0][1:-2])
    
    return target_file_name


# helper function to make a list of items
def make_rough_S3_file_names_list(s3_bucket, target_directory):
    """
    Make a crude list of file contents that needs to be further processed later
    """
    # make list for output
    output_list = []

    # iterate through AWS bucket objects
    for obj in s3_bucket.objects.filter(Prefix= target_directory ):

        # make a list of just stripped-down file names
        output_list.append( get_file_name( str(obj) ) )
    
    # remove empty item from the front of the list
    output_list.pop(0)

    # return the list of file names from the AWS S3 directory
    return output_list


# helper
def make_data_csv_only_file_list(input_list, target_directory):

    new_list = []

    for i in input_list:

        # file only name
        # length = len(target_directory) + 1
        length = len(target_directory)

        name_only = i[length:]

        # # inspection
        # print("name_only", name_only)

        # make comparison-test name
        new_name = target_directory + "metadata_" + name_only 
        # new_name = target_directory + "/" + "metadata_" + name_only 

        # # inspection
        # print("new_name", new_name)

        # make new measure for offsetting the length of 'metadata_' and the folder
        length_plus = length + 9

        # # inspection
        # print(i[length:length_plus])

        # check if file is (not) a metadata_ file
        if "metadata_" != i[length:length_plus]:
            new_list.append(i)

    return new_list

# helper
## like other function but only plain names with no prefixes etc.
def make_plain_names_list(input_list, target_directory):

    new_list = []

    for i in input_list:

        # file only name
        length = len(target_directory)
        # length = len(target_directory) + 1
        name_only = i[length:]

        # # inspection
        # print("plain_name_only", name_only)

        # make comparison-test name
        new_name = target_directory + "metadata_" + name_only 
        # new_name = target_directory + "/" + "metadata_" + name_only 

        # # inspection
        # print("new_plain_name", new_name)

        # make new measure for offsetting the length of 'metadata_' and the folder
        length_plus = length + 9

        # # inspection
        # print(i[length:length_plus])

        # check if file is a meta-data file
        if "metadata_" == i[length:length_plus]:
            # do not add file to list if it is a metadata file
            pass

        else:    
            # add all plain named data files to list
            new_list.append(i[length:])

    # # inspection
    # print("plain_names", new_list)

    return new_list    

# helper
def make_unpaired_data_csv_file_list(input_list, target_directory):

    new_list = []

    for i in input_list:

        # file only name
        length = len(target_directory)
        # length = len(target_directory) + 1
        name_only = i[length:]

        # # inspection
        # print("unpaired_name_only", name_only)

        # make comparison-test name
        new_name = target_directory + "metadata_" + name_only
        # new_name = target_directory + "/" + "metadata_" + name_only 

        # # inspection
        # print("new_name", new_name)

        # make new measure for offsetting the length of 'metadata_' and the folder
        length_plus = length + 9

        # # inspection
        # print(i[length:length_plus])

        # check if file is a meta-data file
        if "metadata_" == i[length:length_plus]:
            # do not add file to list if it is a metadata file
            pass

        elif new_name not in input_list:
            # add file to list if it does not already have a metadata file
            new_list.append(i)

    return new_list

###########################
# main AWS lambda function
###########################
def lambda_handler(event, context):


    ###################################
    # Preemptive Clean Up Lambda /tmp/
    ###################################
    # Clear AWS Lambda Function /tmp/ directory
    clear_tmp_directory()


    ############
    # Get Input 
    ############
    # if not, give user a clear error

    # get from_to_in_csv_flag 
    # True or False
    # Test for input:
    # Note: convert from string to boolean type
    try:
        from_to_in_csv_flag = event["from_to_in_csv_flag"]

        # String to boolean
        if from_to_in_csv_flag == "True":
            from_to_in_csv_flag = True
        else:
            from_to_in_csv_flag = False

        # For terminal
        print("FLAG! from_to_in_csv_flag: ", from_to_in_csv_flag)

    except:
        from_to_in_csv_flag = False


    # get FROM_here_in_csv 
    # True or False
    # Test for input:
    try:
        FROM_here_in_csv = int( event["FROM_here_in_csv"] )
        to_flag = True

    except:
        from_to_in_csv_flag = False
        from_flag = False
        to_flag = False

    # get TO_here_in_csv 
    # True or False
    # Test for input:
    try:
        TO_here_in_csv = int( event["TO_here_in_csv"] )
        from_flag = True

    except:
        from_to_in_csv_flag = False
        from_flag = False
        to_flag = False
    
    if from_flag is True & to_flag is True:
        from_to_in_csv_flag = True


    # get multi_part_or_split_csv_flag 
    # True or False
    # Test for input:
    # Note: convert from string to boolean type
    try:
        multi_part_or_split_csv_flag = event["multi_part_or_split_csv_flag"]

        # String to boolean
        if multi_part_or_split_csv_flag == "True":
            multi_part_or_split_csv_flag = True
        else:
            multi_part_or_split_csv_flag = False

        # For terminal
        print("FLAG! multi_part_or_split_csv_flag: ", multi_part_or_split_csv_flag)
        print("flag type: ", type(multi_part_or_split_csv_flag))

    except:
        multi_part_or_split_csv_flag = False

        # For terminal
        print("FLAG! multi_part_or_split_csv_flag: ", multi_part_or_split_csv_flag)
        print("flag type: ", type(multi_part_or_split_csv_flag))


    # get directory_name in s3
    # Test for input:
    try:
        target_directory = event["target_directory"]

        # check that target directory is followed by a '/'
        if target_directory[-1] != '/':
              target_directory = target_directory + '/'

    except Exception as e:
 
        output = f"""Error: No input for (field=)directory_name 
        Error Message = '{str(e)} 
        """
        
        # print for terminal
        print(output)

        statusCode = 403

        # End the lambda function
        return {
            'statusCode': statusCode,
            'body': output
        }


    # get S3_BUCKET_NAME in s3
    # Test for input:
    try:
        S3_BUCKET_NAME = event["S3_BUCKET_NAME"]

    except Exception as e:
 
        output = f"""Error: No input for (field=)S3_BUCKET_NAME
        Error Message = '{str(e)} 
        """
        
        # print for terminal
        print(output)

        statusCode = 403

        # End the lambda function
        return {
            'statusCode': statusCode,
            'body': output
        }



      
    # get default_folder_for_completed_csv_files
    # Test for input:
    try:
        default_folder_for_completed_csv_files = event["default_folder_for_completed_csv_files"]

        # check that target directory is followed by a '/'
        if default_folder_for_completed_csv_files[-1] != '/':
              default_folder_for_completed_csv_files = default_folder_for_completed_csv_files + '/'

        # print for terminal
        print("User input found for default_folder_for_completed_csv_files = ", default_folder_for_completed_csv_files )

    except:
        # if no input given, then use the default:
        default_folder_for_completed_csv_files = """default_folder_for_completed_csv_files/"""

        # print for terminal
        print("Using default: default_folder_for_completed_csv_files = ", default_folder_for_completed_csv_files )

    ########################################
    # maybe needed still for very big files?
    ########################################

    # # for the part of the .csv by row to copy into AWS
    # # Test for input:
    # # if there is no input for either or both, 
    # # then the function will default to running the whole .csv file
    # try:
    #     start_here = event["start_here"] 
    #     stop_here_inclusive = event["stop_here_inclusive"]

    #     use_whole_file_flag = False

    # except Exception as e:
 
    #     use_whole_file_flag = True


    ######################
    # Connect to DynamoDB
    ######################
    try:
        # connect to AWS DynamoDB with BOTO3
        dynamodb_client = boto3.client('dynamodb')

    except Exception as e:
 
        output = f"""
        Error: Could not connect to DynamoDB, 
        Error Message = '{str(e)} 
        """
        
        # print for terminal
        print(output)

        statusCode = 403

        # End the lambda function
        return {
            'statusCode': statusCode,
            'body': output
        }

    ###############################################
    # S3: Connect to S3 (Make resource and client)
    ###############################################

    try:
        # Set Constants
        AWS_REGION = "us-east-1"

        # make s3_resource
        s3_resource = boto3.resource("s3", region_name=AWS_REGION)

        # make S3 client
        s3_client = boto3.client('s3')

        s3_bucket = s3_resource.Bucket(S3_BUCKET_NAME)


    except Exception as e:
 
        output = f"""Error: Could not connect to AWS S3.
        Error Message = '{str(e)} 
        """
        
        # print for terminal
        print(output)

        statusCode = 403

        # End the lambda function
        return {
            'statusCode': statusCode,
            'body': output
        }


    #####################################################
    # S3: Make Lists of .CSV and Metadata_ files from S3
    #####################################################
    # uses helper functions

    try:
        #############################################################
        # Get all file names from s3 directory (and sub-directories)
        #############################################################

        rough_s3_file_names_list = make_rough_S3_file_names_list( s3_bucket, target_directory )


        # # Note: the code below is turned off by default assuming 
        # # sub-directory handling is not needed. AWS is not friendly for this.
        # # but it works...sometimes. 
        
        # list_all = make_rough_S3_file_names_list( s3_bucket, target_directory )

        # #################################################
        # # RNO SUBS: Remove files from s3 sub-directories
        # #################################################

        # list_of_subdirectories = get_S3_subdirectories_list(s3_client, S3_BUCKET_NAME, target_directory)

        # rough_s3_file_names_list = remove_subdirectory_files_from_list(list_all, list_of_subdirectories)

        #################################
        # Make a data_csv_only_file_list
        #################################
        data_csv_only_file_list = make_data_csv_only_file_list( rough_s3_file_names_list, target_directory )

        #####################################################################
        # Make a list of unpaired file names to use for making table names
        #####################################################################
        unpaired_data_csv_file_list = make_unpaired_data_csv_file_list( rough_s3_file_names_list, target_directory )

        #####################################################################
        # Make a list of just short file names to use for making table names
        #####################################################################
        plain_names_list = make_plain_names_list( rough_s3_file_names_list, target_directory )

        #####################################################
        # Make List of .csv files that NEED a metadata_ file
        #####################################################
        files_that_need_metadata_files_list = []

        # iterate through all data-files
        for this_name in plain_names_list:

            # make the name that a metadata_ file WOULD have
            matching_name = target_directory + "metadata_" + this_name
         
            # if a data file does not have a matching metadata_file, add it
            if matching_name not in rough_s3_file_names_list:
                # add to list 
                files_that_need_metadata_files_list.append( this_name )


    except Exception as e:
 
        output = f"""Error: Could not read file lists from S3
        Error Message = '{str(e)} 
        """
        
        # print for terminal
        print(output)

        statusCode = 403

        # End the lambda function
        return {
            'statusCode': statusCode,
            'body': output
        }


    #########################################
    # make cleaned_aws_names_for_tables_list
    #########################################
    
    cleaned_aws_names_for_tables_list = []

    # remove "split" from root name
    for this_name in plain_names_list:
        # use helper function to remove "split###" from the names of split files
        cleaned_aws_names_for_tables_list.append( remove_split_from_name(this_name) )

    # remove duplicates
    cleaned_aws_names_for_tables_list = list( set( cleaned_aws_names_for_tables_list ) )

    #################################################
    # make a list of tables that may need to be made
    #################################################

    list_of_tables_that_need_to_be_made = []

    for this_name in cleaned_aws_names_for_tables_list:
        # use helper function to remove "split###" from the names of split files
        list_of_tables_that_need_to_be_made.append( this_name[:-4] )

    # # inspection
    # print( "list_all", list_all )
    # print("list_of_subdirectories", list_of_subdirectories)
    print( "LISTS! LOVE IT!" )
    print( "rough_s3_file_names_list", rough_s3_file_names_list )
    print( "unpaired_data_csv_file_list", unpaired_data_csv_file_list )
    print( "data_csv_only_file_list", data_csv_only_file_list )
    print( "plain_names_list", plain_names_list )
    print( "files_that_need_metadata_files_list", files_that_need_metadata_files_list )
    print( "cleaned_aws_names_for_tables_list", cleaned_aws_names_for_tables_list )
    print( "list_of_tables_that_need_to_be_made", list_of_tables_that_need_to_be_made )




    ########################################################################
    # Test AWS-DynamoDB if the tables that you want to create already exist
    ########################################################################
    # if they do does: pass error and tell user 

    # Skip this step if the from_to is turned-on:
    # in cases where from_to is used, the table already exists
    if multi_part_or_split_csv_flag is False:

        # TODO: this should be all plain names...
        for this_name in cleaned_aws_names_for_tables_list:

            # remove the .csv
            this_name = this_name[:-4]

            try:
                response = dynamodb_client.describe_table(TableName=this_name)
            
                output = f"""Error: For *{this_name}* 
                The {this_name} table you want to create ALREADY EXISTS. 
                Please re-name your {this_name}.csv file with an unused table-name. 
                """
                statusCode = 403
                body = "found same-name table"

                # End the lambda function
                return {
                    'statusCode': statusCode,
                    'body': output
                }
                
            except Exception:
                print(f"""OK! "{this_name}" OK! Table does not exist yet for file/name: "{this_name}", Safe to proceed.""")
                body = "OK: Table does (tables do) not exist yet"



    ###############
    # For: From To
    ###############

    if multi_part_or_split_csv_flag is True:

        # iterate through the cleaned aws tables names list (not "split")
        for this_name in cleaned_aws_names_for_tables_list:

            # remove the .csv
            this_name_no_csv_suffix = this_name[:-4]

            try:
                response = dynamodb_client.describe_table(TableName=this_name_no_csv_suffix)
            
                table_needs_to_be_made_flag = False

                # for terminal
                print(f"""multi-part mode: OK! This table exists:"{this_name_no_csv_suffix}" OK! """)

                list_of_tables_that_need_to_be_made.remove(this_name_no_csv_suffix)
                
            except:
                table_needs_to_be_made_flag = True

                # for terminal
                print(f"""multi-part mode TODO: This table needs to be made:"{this_name_no_csv_suffix}" """)

    else:
        table_needs_to_be_made_flag = True

    ##############################################################
    # Use the helper-functions above to make metadata_ .csv files
    ##############################################################
    """
    Using the files_that_need_metadata_files_list
    make new metadata_ files so that every file has a pair:
    this uses the Datatype_Tester.py component

    1. iterate through files_that_need_metadata_files_list (OK)
    2. get this_file from s3 (OK)
    3. make a metadata_ this_file
    4. upload metadata_ this_file to S3
    5. delete this_file data file from /tmp/ 
    Keep iterating
    """

    # 1
    for this_file in files_that_need_metadata_files_list:

        #                       * * *
        ######################## \|/
        # make bouquet of names   |/
        ########################  |

        # short name, no padding
        short_name = this_file

        # name of file in AWS-lambda /tmp/
        s3_name_data = target_directory + this_file

        # metadata_ name that the file WILL have
        s3_metadata_name = target_directory + "metadata_" + this_file

        # name of file in AWS-lambda /tmp/
        lambda_tmp_name_data = '/tmp/' + short_name

        # name of file in AWS-lambda /tmp/
        lambda_tmp_name_meta = '/tmp/' + "metadata_" + short_name

        # # inspection
        # print(f"files_that_need_metadata_files_list, expanded: name in s3: {s3_name_data}")

        try:
            # 2
            # Get .csv from S3 (note: not just readable text from it, but the whole file)
            response = s3_client.get_object(Bucket = S3_BUCKET_NAME, Key = s3_name_data)
            raw_csv = response['Body'].read().decode('utf-8')

            # save file in /tmp/ directory because AWS requires 
            with open(lambda_tmp_name_data, 'w') as data:
                data.write(raw_csv)

            # # inspection
            # print("csv load Testing: These are files in AWS:")
            # print_aws_tmp_files()


        except Exception as e:
    
            output = f"""Error: Could not get data .csv file from S3
            Error Message = {str(e)} 
            """
            
            # print for terminal
            print(output)

            statusCode = 403

            # End the lambda function
            return {
                'statusCode': statusCode,
                'body': output
            }


        try:
            # 3
            # make metadata_ file
            make_metadata_csv( lambda_tmp_name_data )

            # # inspection
            # print("Metadata make Testing: These are files in AWS:")
            # print_aws_tmp_files()


        except Exception as e:
    
            output = f"""Error: Could not make new metadata_ .csv files
            Error Message = {str(e)}
            """
            
            # print for terminal
            print(output)

            statusCode = 403

            # End the lambda function
            return {
                'statusCode': statusCode,
                'body': output
            }

        # # inspection
        # print("s3_metadata_name: ", s3_metadata_name)

        ##############################
        # Upload files into S3 bucket
        ##############################

        try:
            # 4
            # Upload file into S3 bucket
            s3_bucket.upload_file(lambda_tmp_name_meta, s3_metadata_name)
            
            # 5
            # remove file from temp to prevent issues because AWS is buggy
            os.remove(lambda_tmp_name_meta) 
            os.remove(lambda_tmp_name_data) 


        except Exception as e:
    
            output = f"""Error: upload .csv file to S3
            Error Message = {str(e)}
            """
            
            # print for terminal
            print(output)

            statusCode = 403

            # End the lambda function
            return {
                'statusCode': statusCode,
                'body': output
            }


    ##############################
    # Load Data into AWS-DynamoDB
    ##############################
    """
    Using the files_that_need_metadata_files_list
      iterate through the files in the list:
    1. load S3 metadata_ file into -> AWS-lambda /tmp/
    2. load S3 csv data file into  -> AWS-lambda /tmp/
    3. create a new AWS dynamoDB table
    4. enter data into table
    5. remove files from AWS-lambda /tmp/
    Keep iterating
    """

    
    for this_file in plain_names_list:

        #                       * * *
        ######################## \|/
        # make bouquet of names   |/
        ########################  |

        # remove _split__###
        cleaned_aws_name = remove_split_from_name( this_file )

        # remove .csv
        table_name = cleaned_aws_name[:-4]

        data_short_name = this_file
        meta_short_name = "metadata_" + this_file

        # name of file in AWS-lambda /tmp/
        s3_name_data = target_directory + data_short_name
        s3_name_meta = target_directory + meta_short_name

        # name of file in AWS-lambda /tmp/
        lambda_tmp_name_data = '/tmp/' + data_short_name
        lambda_tmp_name_meta = '/tmp/' + meta_short_name

        # # inspection
        # print(f"files_that_need_metadata_files_list, expanded: name in s3: {s3_metadata_name}")

        #########################################################
        # Get and load to pandas Data and Metadate files from S3
        #########################################################

        try:
            # 1
            # data
            # Get data.csv from S3 (note: not just readable text from it, but the whole file)
            response = s3_client.get_object(Bucket = S3_BUCKET_NAME, Key = s3_name_data)
            raw_csv = response['Body'].read().decode('utf-8')

            # save file in /tmp/ directory because AWS requires 
            with open( lambda_tmp_name_data, 'w' ) as data:
                data.write( raw_csv )

            # load .csv into pandas dataframe
            data_df = pd.read_csv( lambda_tmp_name_data )

            # # inspection
            # print("Data Testing: These are the files in AWS-Lambda /tmp/:")
            # print_aws_tmp_files()
            # print("testing data_df.head(1)", data_df.head(1) )


            # 2
            # metadata_
            # Get metadata_csv from S3 (note: not just readable text from it, but the whole file)
            response = s3_client.get_object(Bucket = S3_BUCKET_NAME, Key = s3_name_meta)
            raw_csv = response['Body'].read().decode('utf-8')

            # save file in /tmp/ directory because AWS requires 
            with open( lambda_tmp_name_meta, 'w' ) as data:
                data.write( raw_csv )

            # load .csv into pandas dataframe
            meta_df = pd.read_csv( lambda_tmp_name_meta )

            # # inspection
            # print("Meta Testing: These are the files in AWS-Lambda /tmp/:")
            # print_aws_tmp_files()
            # print("testing meta_df.head(1)", meta_df.head(1) )


        except Exception as e:
    
            output = f"""Error: error Get and Load metadata from AWS S3 metadata_file_name_in_s3 .csv from S3 
            Error Message = '{str(e)} 
            """
            # print for terminal
            print(output)

            statusCode = 403

            # End the lambda function
            return {
                'statusCode': statusCode,
                'body': output
            }


        ##################################
        # Primary Key Acceptability Check
        ##################################
        """
        DynamoDB needs a unique primary key column
        Make sure the first column is acceptable as a primary key:
        Report an error flag for each issue you test for:

        - flag report for first column:
          - type == object
          - nuls > 0
          - isna > 0
          - duplicates > 0
        if flag number > 0 halt and report flags
        if no flags: "ok, process, no warning flags on primary key column"
        """
        
        # Run helper function to check for warning flags
        warning_flags_list = make_primary_key_warning_flag_list(data_df)

        # for terminal or inspection
        print( "missing_data_flag: ", warning_flags_list )


        if len(warning_flags_list) != 0:

            output = f"""Error primay key warning for {table_name} with {warning_flags_list}: 
            The first collumn of {table_name} cannot be a primary key.
            Safety-checks returned the following warning flags:
            {warning_flags_list}
            """
            # print for terminal
            print(output)

            statusCode = 430

            # End the lambda function
            return {
                'statusCode': statusCode,
                'body': output
            }

        #############################
        # Make Metadata Dictionary
        #############################

        # Make Metadata Dictionary
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html 
        metadata_dictionary = meta_df.to_dict('index')

        # # inspection
        # print("test dump: metadata_dictionary", metadata_dictionary)


        #############################
        # Make New Table in DynamoDB
        #############################


        # insepction
        print("inspection for section: # Make New Table in DynamoDB line 1464")
        print("multi_part_or_split_csv_flag", type(multi_part_or_split_csv_flag), multi_part_or_split_csv_flag)
        print("table_needs_to_be_made_flag", type(table_needs_to_be_made_flag), table_needs_to_be_made_flag)


        # Do NOT skip this step if: 
        # multi_part_or_split_csv_flag is False) 
        # or (table_needs_to_be_made_flag is True
        if (multi_part_or_split_csv_flag is False) or (table_needs_to_be_made_flag is True):

            if table_name in list_of_tables_that_need_to_be_made:

                try:
                    # get primary key data so that you can make the new table
                    primary_key_name, key_dtype = get_primary_key_dtype_data_to_make_table(meta_df)

                    # make new table in aws-dynamoDB
                    make_table_in_aws_dynamoDB(dynamodb_client, table_name, primary_key_name, key_dtype)

                    # if table made: remove name from the list of tables to make
                    list_of_tables_that_need_to_be_made.remove(table_name)

                except Exception as e:
            
                    output = f"""Error: Could NOT make new table in dynamoDB:
                    this_file = {this_file}
                    table_name = {table_name}
                    primary_key_name = {primary_key_name} 
                    key_dtype = {key_dtype} 
                    Error Message = '{str(e)} 
                    """
                    # print for terminal
                    print(output)

                    statusCode = 403

                    # End the lambda function
                    return {
                        'statusCode': statusCode,
                        'body': output
                    }



        #################################
        # check to see if table is ready
        #################################

        # check status of table, it takes time to create and be ready
        response = dynamodb_client.describe_table(TableName=table_name)

        # # inspection
        # print (str(response))
        # print( regex_boolean_detect_creating(str(response)) )

        # set counter
        counter = 0

        # check if table status is not 'creating'
        while regex_boolean_detect_creating(str(response)) is True:
            
            # Wait
            time.sleep(1)

            # recheck status
            response = str(dynamodb_client.describe_table(TableName=table_name))

            # for terminal
            print( "Still waiting, counter: ", counter )

            # increment counter
            counter += 1

        ####################
        ####################
        # Write to DynamoDB
        ####################
        ####################

        #TODO
        # FROM_here_in_csv
        # TO_here_in_csv
        # if no from-to-default

        if from_to_in_csv_flag is True:
            pass

        else:
            # whole file
            FROM_here_in_csv = 0
            TO_here_in_csv = data_df.shape[0]

        ################################################
        # Iterate table Rows and Load into AWS DynamoDB
        ################################################

        # for each row number in the df
        for row_number in range( FROM_here_in_csv, TO_here_in_csv ):
          
            this_item = make_dynamo_item(data_df, metadata_dictionary, row_number)

            # # inspection
            # print("Table name", table_name)
            # print("This item", this_item)

            try:
                # # connect to the client
                add_to_db = dynamodb_client.put_item(
                    
                    # put your specific table name here:
                    TableName = table_name,

                    # This is the 'item' or 'row' in the database / table
                    Item = this_item
                )



            except Exception as e:
                output = f"""Error: error with one-row upload to dynamoDB:
                row = {row_number}
                file name = {lambda_tmp_name_data} 
                Error Message = '{str(e)} 
                """
                # print for terminal
                print(output)

                statusCode = 403

                # End the lambda function
                return {
                    'statusCode': statusCode,
                    'body': output
                }



        #########################
        # Relocate files in S3
        #########################
        # if data transfer has completed, then move the files into a 'tranfered' directory

        # Skip if from_to is True
        if from_to_in_csv_flag is False:

            try:
                # move data csv
                move_file_in_S3(s3_resource, S3_BUCKET_NAME, target_directory, data_short_name, default_folder_for_completed_csv_files)

                # move metadata_ file
                move_file_in_S3(s3_resource, S3_BUCKET_NAME, target_directory, meta_short_name, default_folder_for_completed_csv_files)

                # for terminal:
                print(f"File Moved after Completed: {data_short_name},{meta_short_name}")


            except Exception as e:
                
                output = f"""Error: Error with relocating files inside S3
                Error Message = '{str(e)} 
                """
                # print for terminal
                print(output)

                statusCode = 430

                # End the lambda function
                return {
                    'statusCode': statusCode,
                    'body': output
                }

    ##############################
    # Final Clean Up Lambda /tmp/
    ##############################
    # Clear AWS Lambda Function /tmp/ directory
    clear_tmp_directory()


    ###############
    # Final Output
    ###############
    status_code = 200
    output = f"""OK! Processes complete for these files: {data_csv_only_file_list}
    See metadata_ files and original files in the new 'transfered_files' directory'
    Please check dynamoDB tables to see that datatypes and data transfer is satisfactory. 
    """

    return {
        'statusCode': status_code,
        'body': output
    }
