########################################################################
### imports ###
########################################################################
from concurrent.futures import process
from distutils.log import debug
import pandas as pd
import json
import bz2

from zipfile import ZipFile
from regex import P
import zstandard
import pickle as pkl
import string
import time
import os
import csv
import datetime
import numpy as np

### Imports for data cleaning steps ###
import nltk
import re
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
# from spellchecker import SpellChecker
nltk.download('words')
from nltk.corpus import stopwords
import os, sys
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from polyglot.detect.base import logger as polyglot_logger
polyglot_logger.setLevel("ERROR")
from polyglot.detect import Detector

if "polyglot" in sys.modules:
    from polyglot.detect.base import logger as polyglot_logger
    polyglot_logger.setLevel("ERROR")
    from polyglot.detect import Detector
else:
    print("--WARNING-- Package will fail if trying to run detect_english")
    print("- used when cleaning data")
    print("- Pip package required: polygot")
    print("- Other linux/windows pre-requists may be requried")

########################################################################
######################### standard  tools ##############################
########################################################################

### import file types ###
def get_pkl(file):
    """
    Opens pkl file
    """
    return(pd.read_pickle(file))


def get_json(file):
    """
    Opens json file
    """
    return(pd.read_json(file, lines=True))


def get_csv(file):
    """
    Opens csv file as list of dictionaries
    """
    with open(file) as csvfile:
        csv_reader = csv.DictReader(csvfile)
        rows = list(csv_reader)

    csvlist = []
    for row in rows:
        csvlist.append(row)
    return(csvlist)


def get_xz(file):
    """
    Opens lzma file (.xz)
    """
    return(lzma.open(file).read())
import lzma


### uncompress files ###
import tarfile
def extract_tar(file, output_dir):
    '''
    open tar file
    '''
    with tarfile.open(file) as f:
        f.extractall(output_dir)


### Misc. tools ###
def merge_dataframes(df_old, df_new):
    """
    Merge two dataframes
    """
    return df_old.append(df_new)


def time_taken(start_time):
    '''
    Print time when that location reached
    '''
    print("    --- %s seconds ---" % str(round((time.time() - start_time),2)))


def platform_path (Platform, Avaliable_Path_Dict):
    '''
    Reality check that selected plaform to run through code exists
    '''
    if Platform in Avaliable_Path_Dict:
        the_path = Avaliable_Path_Dict[Platform]
    else:
        print('ERROR platform name not in "Avaliable_Path_Dict"  - ' + Platform) 
        os.exit ()                                              # this doesnt exist will crash program :)
    return the_path


def extract_log_header(log_file, csv_columns):
    '''
    
    '''
    log_file = open(log_file, 'w')                               # Saving Log to csv
    writer = csv.DictWriter(log_file, fieldnames=csv_columns)
    writer.writeheader()
    return(writer)


def convert_date(datestr, datetype): 
    '''
    Change date to correct format
    forms include utc, dateline, dateslash etc...
    '''
    if datetype == 'utc':
        datestr = pd.to_datetime(datestr, format='%Y%m%d%H%M%S')
    elif datetype == 'str':
        datestr = pd.to_datetime(datestr, unit='s')
    elif datetype == 'dateslash':
        datestr = pd.to_datetime(datestr, format='%d/%m/%Y').date()
    elif datetype == 'dateline':
        datestr = pd.to_datetime(datestr, format='%Y-%m-%d').date()       
    elif datetype == 'datetimeslash':
        datestr = pd.to_datetime(datestr, format='%Y/%m/%d %H%M%S')    
    elif datetype == 'datetimeline':
        datestr = pd.to_datetime(datestr, format='%Y/%m/%d %H%M%S')    
    return(datestr)


### Convert file types ###
def convert_events_csv_to_pkl(Raw_Path, Processed_Path, file_name):
    """ 
    Convert date of event with event occuring and risk of event to dictionary and save as pkl
    date          event        severity
    dd/mm/yyyy    1            0
    type(date)    type(int)    type(int)
    """
    with open(os.path.join(Raw_Path, file_name + '.csv'), mode='r') as csv_file:
        dict_reader = csv.DictReader(csv_file)
        list_of_dict = list(dict_reader)

    dict1 = {}
    for unrest in list_of_dict:
        dateformatted = datetime.datetime.strptime(unrest["date"], "%d/%m/%Y").date()  # Convert date string to type: date        
        event = int(unrest["event"])                                                   # Convert event to integer
        severity = int(unrest["severity"])                                             # Convert serverity to integer

        dict1[dateformatted] = [event, severity]

    with open(os.path.join(Processed_Path, file_name + ".pkl"), 'wb') as handle:
        pkl.dump(dict1, handle, protocol=pkl.HIGHEST_PROTOCOL)




########################################################################
####################### Platform Specific tools ########################
########################################################################


def format_data_reddit2(ldata, output_dir, dte_down, Start_Time, Int_Time, column_header, Make_Changes, Debug_Mode, writer=""):
    """
    Extract, clean, add sentiment
    and save as pkl 
    """
    fname = str(dte_down.hour) + ".pkl"                                     # Output Filename
    year = str(dte_down.year)
    month = str(dte_down.month)
    dir_name = str(dte_down.day)
    
    output_date_dir = os.path.join(output_dir, year, month, dir_name)

    if Make_Changes:
        os.makedirs(output_date_dir, exist_ok=True)                         # Make folders that do not exist

    pdl = pd.DataFrame.from_dict(ldata)                                     # Convert to dataframe
    raw_dataset = extract_data_reddit(pdl)      

    ### clean the data before it is saved ###
    if Debug_Mode:
        print('  Cleaning the data')

    raw_dataset = cleaning_2(raw_dataset, Start_Time, Debug_Mode, column_header)

    ### Add sentiment ###
    if Debug_Mode:
        print('  Adding sentiment')

    raw_dataset = sentiCalc3(raw_dataset, Start_Time, Debug_Mode, column_header) 
    if Debug_Mode:
        time_taken(Start_Time) 

    if Debug_Mode:
        print(raw_dataset)
        print(raw_dataset.info())

    ### If all data is removed in cleaning, return to format_data_reddit without saving ###
    if  raw_dataset.shape[0] == 0 :
        return raw_dataset   

    ### Make saves ###
    if Make_Changes:
        File_Path = os.path.join(output_date_dir, fname)
        if os.path.exists(File_Path):           # append files if already exist
            Old_Data = get_pkl(File_Path)
            All_Data = merge_dataframes(Old_Data, raw_dataset)
            All_Data.to_pickle(File_Path)         # Save to file

        else:
            raw_dataset.to_pickle(File_Path)         # Save to file

    ### for the log file ###
    
    print("Saving File: " + dir_name + "/" + fname + "  --  lines: " + str(raw_dataset.shape[0]) + "  --  Dropped: " +  
        str(len(ldata) - raw_dataset.shape[0]) + "(" + str(round((len(ldata) - raw_dataset.shape[0]) / len(ldata) * 100, 2) ) + "%)  --  " + 
        str(round(time.time() - Int_Time, 2)) + "s(" + str(round(time.time() - Start_Time)) + ")  --  Start: " + str(dte_down))

    log_dict = {}                                                           # Log Data to save to CSV
    log_dict["dir_name"]    = dir_name
    log_dict["fname"]       = fname
    log_dict["start_time"]  = str(dte_down)
    log_dict["raw_count"]   = len(ldata)
    log_dict["final_count"] = str(raw_dataset.shape[0])
    log_dict["Dropped"]     = str(round((len(ldata) - raw_dataset.shape[0]) / raw_dataset.shape[0] * 100, 2))
    log_dict["run_time"]    = str(round(time.time() - Int_Time, 2))
    log_dict["cumulative_time"] = str(round(time.time() - Start_Time, 2))
    log_list = []
    log_list.append(log_dict)                                               # Append Dict to list
    if Make_Changes:
        writer.writerows(log_list)                                          # Save list of dict To CSV


def extract_data_parler(pdl): 
    '''
    Change data to desired format and columns
    '''
    pdl["text"] = pdl["body"]                                       # Rename body of text column 
    pdl = pdl[pdl['text'].str.strip().astype(bool)]                 # Remove rows where text is blank
    pdl["created_utc"] = pd.to_datetime(pdl["createdAt"], format='%Y%m%d%H%M%S') 
    pdl['publicationDate'] = pdl["created_utc"].dt.date             # Extract date from created_at 
    pdl['publicationTime'] = pdl["created_utc"].dt.time             # Extract time from created_at 
    raw_dataset = pdl[['text', 'id', "created_utc", 'publicationDate', 'publicationTime','hashtags']]    # Extract columns from pd
    return(raw_dataset)


def extract_data_reddit(pdl): 
    '''
    Change data to desired format and columns, 
    drop deleted comments 
    '''
    pdl.drop(pdl.loc[pdl['author']=="[deleted]"].index, inplace=True)       # Remove rows which have been deleted                          # Remove rows which have posts that have been deleted  
    pdl["text"] = pdl["body"]        
    pdl["created_utc"] = pd.to_datetime(pdl["created_utc"], unit='s')                                                        # rename column with comments 
    pdl['publicationDate'] = pdl["created_utc"].dt.date                     # Extract date from created_at 
    pdl['publicationTime'] = pdl["created_utc"].dt.time                     # Extract time from created_at 
    raw_dataset = pdl[['text', 'id', "created_utc", 'publicationDate', 'publicationTime']]  # Extract columns from pd
    return(raw_dataset)


def extract_data_twitter(pdl): 
    '''
    Change data to desired format and columns, 
    drop deleted comments 
    '''
    pdl.drop(pdl.loc[pdl['author']=="[deleted]"].index, inplace=True)       # Remove rows which have been deleted                          # Remove rows which have posts that have been deleted  
    pdl["text"] = pdl["body"]        
    pdl["created_utc"] = pd.to_datetime(pdl["created_utc"], unit='s')                                                        # rename column with comments 
    pdl['publicationDate'] = pdl["created_utc"].dt.date                     # Extract date from created_at 
    pdl['publicationTime'] = pdl["created_utc"].dt.time                     # Extract time from created_at 
    raw_dataset = pdl[['text', 'id', "created_utc", 'publicationDate', 'publicationTime']]  # Extract columns from pd
    return(raw_dataset)


########################################################################
####################### Universal tools ################################
########################################################################


def cleaning_2(raw_dataset, Start_Time, Debug_Mode, column_header, topPercent=0.01, bottomPercent=0.2):
    '''
    clean the dataset text column
        make lower case, 
        remove emoji, 
        remove punctuation,
        remove non english words,
        Spell check,
        Lemmatize text,
        Remove stop words (update newStopWords to include new words to be removed)
        Remove ends of word frequency,
        Drop rows where no text exists
        Add sentiment
    '''
    ## make column lower case
    raw_dataset[column_header] = raw_dataset[column_header].str.lower()

    ### remove emoji
    filter_char = lambda c: ord(c) < 256
    raw_dataset[column_header] = raw_dataset[column_header].apply(lambda s: ''.join(filter(filter_char, s)))

    ### Remove punctuation  ###
    raw_dataset[column_header] = raw_dataset[column_header].apply(lambda x:''.join([i for i in x if i not in string.punctuation]))
    
    ### Remove non english words ### 
    englishwords = set(nltk.corpus.words.words())
    raw_dataset[column_header] =  raw_dataset[column_header].str.split(' ').apply(lambda x: ' '.join(k for k in x if k.lower() in englishwords or not k.isalpha()))
    
    if Debug_Mode:
        print('    Remove non english words')
        time_taken(Start_Time) 
   
    # ### Spell check ###
    # raw_dataset[column_header] = raw_dataset[column_header].str.lower().apply(lambda txt: ''.join([SpellChecker().correction(txt)])) 
    
    # if Debug_Mode:
    #     print('Spell check')
    #     time_taken(Start_Time) 

    ### Lemmatize text  ###
    raw_dataset[column_header] = raw_dataset[column_header].apply(lambda x: ''.join(lemmaWordsinString(x)))
    if Debug_Mode:
        print('    Lemmatize text ')
        time_taken(Start_Time)

    ### Remove stop words. both generic and custom ###
    cachedStopWords = stopwords.words('english')
    newStopWords = ['rt', 'en','parlerconcierge', 'puedo','app']                                                       # Add words as required 
    cachedStopWords.extend(newStopWords)
    raw_dataset[column_header] = raw_dataset[column_header].str.split(' ').apply(lambda x: ' '.join(k for k in x if k.lower() not in cachedStopWords))

    if Debug_Mode:
        print('    Remove stopwords')
        time_taken(Start_Time)

    ### Remove words based on frequency ###  https://michael-fuchs-python.netlify.app/2021/06/16/nlp-text-pre-processing-vi-word-removal/#removing-frequent-words 
    word_rankings    = pd.Series(' '.join(raw_dataset[column_header]).lower().split()).value_counts()           # rank words by frequency 
    len_topPercent = int(len(word_rankings)*topPercent)                                                         # number of words to remove from top of word_rankings
    len_bottomPercent = int(len(word_rankings)*bottomPercent)                                                   # number of words to remove from bottom of word_rankings
    top_percent_words = word_rankings.head(len_topPercent)                                                      # most frequent words to be removed   
    bottom_percent_words = word_rankings.tail(len_bottomPercent)                                                # least frequent words to be removed
    remove_word_list     = pd.concat([top_percent_words,bottom_percent_words])                                  # list of words to be removed
    raw_dataset[column_header] = raw_dataset[column_header].str.split(' ').apply(lambda x: ' '.join(k for k in x if k.lower() not in remove_word_list))  # remove identified words from each row
    
    if Debug_Mode:
        print('    Remove top and bottom frequency words')
        time_taken(Start_Time)

    ### Drop rows where all text has been removed ###
    raw_dataset = raw_dataset[raw_dataset[column_header].str.strip().astype(bool)]

    if Debug_Mode: 
        print('    Remove empty rows')    

    ### Print text column to have a look  
    if Debug_Mode:
        print(raw_dataset[column_header])
    
    return raw_dataset


def sentiCalc3(raw_dataset, Start_Time, Debug_Mode, column_header='text'):
    '''
    This function returns a new dataframe of tweets from the old with the added column sentiment
    '''
    
    indo_EngsentEnd = raw_dataset.count()[0]   # this just gets the number of tweets there are

    indo_EngTweetsPd_sent = raw_dataset.copy()
    indo_EngTweetsPd_sent["sentiment"] = ""  # add empty column named sentiment
    sid = SentimentIntensityAnalyzer()
    labels = [indo_EngTweetsPd_sent.index[c] for c in range(indo_EngsentEnd)]
    for f in labels:

        tense = indo_EngTweetsPd_sent.loc[f].text

        ss = sid.polarity_scores(tense) 
        theSent = ss["compound"]             # note neg, neu, pos, compound

        indo_EngTweetsPd_sent.loc[f,'sentiment'] =(theSent)
        indo_EngTweetsPd_sent.loc[f, column_header] = tense


    return(indo_EngTweetsPd_sent)


def lemmaWordsinString(strinput):
    wnl = WordNetLemmatizer()
    keys = re.findall(r"[\w']+", strinput)
    #keys = [wnl.lemmatize(w) for w in keys]
    keys = [wnl.lemmatize(w, 'v') for w in keys] # add 'v' so that 'loving' is 'love', however, still not work for 'could'
    tmp = ' '.join(keys)
    return tmp


def detect_en(text):
    '''
    Remove any rows were text is not in english 
    '''
    try:
        return Detector(text).language.code == 'en'
        # return detect(text) == 'en'
    except:
        return False


########################################################################
#####################Merge dataset tools ###############################
########################################################################

def merge_dataset (config, inputdir, keyword_list=False, sample_size=False):
    '''
    Merge files in folder to new file 
    sampling fo the dataset if required
    return the datset (does not save)
    '''
    temp_data = []                                                                                      # Create empty DF
   
    for root, subdirs, files in os.walk(inputdir):
        for file in files:

            if os.path.splitext(file)[1] == '.pkl':
                file_path = os.path.join(root, file)
                newDF = pd.read_pickle(file_path)
                temp_data.append(newDF)

    df = pd.concat(temp_data, ignore_index=True)                                                        # Concatenate pkl files to one
    
    ### if sample size present sample data ###
    if sample_size:
        df = sample_dataset(df, keyword_list, sample_size)

    return(df)


def sample_dataset (df, keyword_list, sample_size=1000):
    '''
    Create a stratified subsample of the data, based on publication date
    If input data less than sample_size return unsampled dataset
    '''
    # print(df)
    # print(len(df))
    # print((df))
   
    ### remove netrual rows. ie when sentiment is between -0.2 and 0.2 ###
    df = df[(df['sentiment'] <= -0.2) | (df['sentiment'] >= 0.2)]

    ### Keep rows where key word exists ###
    if keyword_list: 
        def word_checker(sentence):
            return any(word in keyword_list for word in sentence.lower().split())

        df = df[df['text'].apply(word_checker)]
    
    
    if len(df) >= sample_size :
        ### stratified sampling. by proportion of data ###
        df = df.groupby('publicationDate', group_keys=False)  \
                    .apply(lambda x: x.sample(int(np.rint(sample_size*len(x)/len(df)))))  \
                    .sample(frac=1).reset_index(drop=True)  

    print(len(df))
    return(df)


def merge_daily_to_year_file (config, input_dir, output_dir):
    '''
    Create dataset of all daily files into one year file
    '''
    for year in sorted(os.listdir(input_dir)):
    # for year in ['2020']:        
        yearpath = os.path.join(input_dir, year)
        if os.path.isdir(yearpath):
            
            if config['Debug_Mode']:
                print('year')
                print(year)
            
        ### merge and save by year
        input_file_dirY = os.path.join(input_dir, year)                                          # Define new file dir (for year)
        
        new_fileY = os.path.join(output_dir, year + '.pkl')                               # Define file name (for year)
        print(input_file_dirY)
        print(new_fileY)
        
        output_data = merge_dataset(config, input_file_dirY) 

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)                         # Make folders that do not exist
            output_data.to_pickle(new_fileY)
        

def merge_dataset_save(file_path_in1, file_path_in2, file_path_out, save=True):
    '''
    Merge files in folder to new file 
    This is mainly used to merge the Parler data which was run in parallel
    '''

    pd1 = pd.read_pickle(file_path_in1)
    pd2 = pd.read_pickle(file_path_in2)

    df = {}
    if save:
        df = pd.concat([pd1, pd2], ignore_index=True)                                                        # Concatenate pkl files to one
        df.to_pickle(file_path_out)

    return(df)

########################################################################
########################################################################
########################################################################
