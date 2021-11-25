 ## Helper Colab cell to load a .csv and create a draft of a metadata-csv

# import library
import pandas as pd
import glob 

# get name of file from user
# name_of_csv = input("What is the name of your .csv?")

# helper function 
def make_metadata_csv(name_of_csv):
    '''
    This function makes a metadata_ file
    with three kinds of data:
    1. the pandas datatype for each column
    2. the AWS dynamoDB data type for each column
    3. an example of the data for each column

    Requires: pandas as pd
    '''

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
    # new_name_of_csv = "/tmp/metadata_" + name_of_csv[5:]

    # # for normal OS or python notebook
    new_name_of_csv = "metadata_" + name_of_csv

    '''
    Below are two different versions of formatted output
    in terms of the structure
    of the resulting .csv file
    '''

    # # saving the dataframe
    # df_meta.to_csv( new_name_of_csv )

    # # saving the dataframe (alternate version)
    df_meta.to_csv( new_name_of_csv , header=True, index=False)

    # delete dataframes to save memory
    #del df
    #del df_meta

    # end program
    return None


# Helper Function
def make_primary_key_warning_flag_list(file_name):

    # load file into pandas dataframe
    df = pd.read_csv( file_name )

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

    return print(warning_flag_list)

# in the case of a long wait, give the user some idea
# of the progress through the files (crude but works)
progress_counter = 0
# inspection
# print("progress Counter:")

# TODO
# import glob 
list_of_csv_files = glob.glob('*.csv', recursive = True)

# iterate through all .rds files in directory
the_path = "."
for filename in list_of_csv_files:

    # inspection
    # print(filename)

    # find AWS data types
    make_metadata_csv(filename)

    # make 
    make_primary_key_warning_flag_list(file_name)

    # Show Progress:
    progress_counter += 1
    print(f"{ progress_counter }/{ len(list_of_csv_files) }")

# list of metadata files
list_of_metadata_files = glob.glob('metadata_*.csv', recursive = True)


# Yay!!
print("All Done!!")

# may take extra time to print
# print( "List of new files = 
", glob.glob('metadata_*.csv', recursive = True) )
