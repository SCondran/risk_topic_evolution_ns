


#**************************************************************************************
#...................................Format the data.......................................
#**************************************************************************************


'''
1. Read in raw data
2. Uncompress raw data
3. Drop and formate columns as required remove deleted comments
4. Sort data into year, month, day folders and hourly file
5. Clean data
6. Add sentiment
7. Save as pkl
'''

import time
import os, json, zstandard, time, csv, io, datetime
from numpy import not_equal 
import pandas as pd
from . import sc_tools

def format_data_reddit(config, full_file_name, runfolder='', column_header='text'):
    '''
    note the raw data files can be mixed chronologically. 
    read in zst raw data, 
    clean the dataset, add sentiment score 
    save {dataset}/2format/{year}/{month}/{day}/{hour}.pkl
    '''
    if config['Make_Changes']:
        os.makedirs(config['output_dir'], exist_ok=True)                         # Make folders that do not exist
    
    log_name =  "/log.csv"

    if runfolder: 
        log_name =  "/log" + str(runfolder) + '.csv'
    log_file = config['output_dir'] + log_name
    input_file  = os.path.join(config['input_dir'], full_file_name)
    # file_name = os.path.splitext(full_file_name)[0]   

    Start_Time = time.time()

    ### Logging ###
    writer = ''
    if config['Make_Changes']:
        csv_columns = ["dir_name", "fname", "start_time", "raw_count", "final_count", "Dropped", "run_time", "cumulative_time"]
        writer = sc_tools.extract_log_header(log_file, csv_columns)


    ### Run Code ###
    ldata = []                                                                                              # When code is extracted from file its added to this list
    First_Run = True
    with open(input_file, 'rb') as fh:                                                                      # Open the compressed file
        dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
        with dctx.stream_reader(fh) as reader:                                                              # A context manager
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            for line in text_stream:                                                                        # For each Record in the Data stream
                data = json.loads(line)

                ### First Run In Bin ###
                if First_Run:                                                                               # Only runs first time or next time AFTER a file has been created (aka start of time bin)
                    First_Run = False
                    dte = datetime.datetime.utcfromtimestamp(data["created_utc"])                           # Convert time epoch to datetime
                    dte_down = dte.replace(minute=0, second=0, microsecond=0)                               # Round down to hour (Remove mins/seconds)
                    dte_up = dte.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)   # Round up to hour (Remove mins/seconds and add 1hr)
                    EP_Next_Hour = dte_up.timestamp()                                                       # Convert rounded up to epoch
                    Int_Time = time.time()

                    if config['Debug_Mode']:    
                        print(dte_down)



                ### Last Run in Bin ###
                if data["created_utc"] > EP_Next_Hour:                                                     # If falls in the hour bin and more then 100 records in list (hack to stop multiple being records being exactly on the hour creating their own file)
                    if ldata: 
                        sc_tools.format_data_reddit2(ldata, config['output_dir'], dte_down, Start_Time, Int_Time, column_header, config['Make_Changes'],config['Debug_Mode'], writer)
                        
                        ldata = []                          # Clear Data Bin
                        First_Run = True 
              
                ### Add the Data to the List ###
                ldata.append(data)                      # Add it to List (Time Bin)
    
    ### Save Last Bin ###
        if ldata: 
            print('last row data')
            sc_tools.format_data_reddit2(ldata, config['output_dir'], dte_down, Start_Time, Int_Time, config['Make_Changes'],config['Debug_Mode'], writer, column_header)


    # if config['Make_Changes']:
    #     log_file.close()
    gc.collect()

import gc




def format_data_parler(config, full_file_name, runfolder='', column_header='text'):
    '''
    read in zst raw data, remove delete rows, 
    save as pkl by year, month, day, hour
    '''
    if config['Make_Changes']:
        os.makedirs(config['output_dir'], exist_ok=True)                         # Make folders that do not exist
    
    ### Log name ###
    log_name =  "log.csv"
    ##### I manually split the raw data into run# so as to run in multiple terminals. #####
    if runfolder: 
        log_name =  "log" + str(runfolder) + '.csv'
    log_file = os.path.join(config['output_dir'], log_name)
    input_file = os.path.join(config['input_dir'], full_file_name)
    file_name = os.path.splitext(full_file_name)[0]  

    Start_Time = time.time()
    ### Logging ###
    writer = ''
    if config['Make_Changes']:
        csv_columns = ["dir_name", "fname", "start_time", "raw_count"]
        writer = sc_tools.extract_log_header(log_file, csv_columns)

    ### Run Code ###
    loaded_data = sc_tools.get_json(input_file)
    loaded_data = sc_tools.extract_data_parler(loaded_data)

    print("  Rows: " + format(loaded_data.shape[0],',d') + "  -  ", end = '')

    The_Data_List = loaded_data

    ### Save Data to separate files year/month/day/hour.pkl###
    Start_hr = min(The_Data_List["created_utc"]).replace(minute=0, second=0, microsecond=0)
    Max_date = max(The_Data_List["created_utc"]).replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)

    loop_start_time = Start_hr
    while True:
        loop_end_time = loop_start_time + datetime.timedelta(hours=1)

        if loop_start_time > Max_date:  # Stop code when true
            break
        
        print("  From: " + str(loop_start_time) + "  -  To: " + str(loop_end_time))

        ## Filter data fror date range ##
        raw_dataset = The_Data_List.loc[(The_Data_List["created_utc"] >= loop_start_time) & (The_Data_List["created_utc"] < loop_end_time)]

        if config['Debug_Mode']:
            print(raw_dataset.info())

        ### clean the data before it is saved ###
        if config['Debug_Mode']:
            print('  Cleaning the data')

        raw_dataset = sc_tools.cleaning_2(raw_dataset, Start_Time, config['Debug_Mode'], column_header)


        if config['Debug_Mode']:
            sc_tools.time_taken(Start_Time) 


        ### Add sentiment ###
        if config['Debug_Mode']:
            print('  Adding sentiment')

        raw_dataset = sc_tools.sentiCalc3(raw_dataset, column_header, config['Debug_Mode'], Start_Time) 
        
        if config['Debug_Mode']:
            sc_tools.time_taken(Start_Time)

        ### Have a look at the data
        if config['Debug_Mode']:
            print(raw_dataset)
            print(raw_dataset.info())

        New_Data = raw_dataset

        ## If dataframe Not Empty - save to file ##
        if not New_Data.empty:
            New_FileName = str(loop_start_time.hour) + ".pkl"
            New_Out_Path = os.path.join(config['output_dir'], str(loop_start_time.year), str(loop_start_time.month), str(loop_start_time.day))
            New_Out_FilePath = os.path.join(New_Out_Path, New_FileName)
            print("  " + New_Out_Path)

            if config['Debug_Mode']:
                print(New_Data)
                print(New_Data.info())
            if config['Make_Changes']:
                os.makedirs(New_Out_Path, exist_ok=True)                            # Make folders that do not exist
                if os.path.exists(New_Out_FilePath):
                    previous_data = sc_tools.get_pkl(New_Out_FilePath)
                    all_data = previous_data.append(New_Data)
                    all_data.to_pickle(New_Out_FilePath)
                else:
                    New_Data.to_pickle(New_Out_FilePath)



            ## Log File Stuff ##
            log_dict = {}                                                           # Log Data to save to CSV
            log_dict["dir_name"]    = New_Out_Path
            log_dict["fname"]       = New_FileName
            log_dict["start_time"]  = str(loop_start_time)
            log_dict["raw_count"]   = len(New_Data)
            log_list = []
            log_list.append(log_dict)                                               # Append Dict to list
            if config['Make_Changes']:
                writer.writerows(log_list)                                          # Save list of dict To CSV


        ## Final stage of the loop to add 1 hour ##
        loop_end_time = loop_start_time
        loop_start_time += datetime.timedelta(hours=1)


    # if config['Make_Changes']:
    #     log_file.close()

    # print(The_Data_List)
    # print(list(The_Data_List.columns))




#**************************************************************************************
#...................................Create dataset.....................................
#**************************************************************************************
 

def create_sampled_dataset_daily (config, input_dir, output_dir, keyword_list=False, sample_size=False):
    '''
    Some daily datasets are to big to process with current computing power. 
    If the data is to big then code will error out
    This block of code will sample a subset of the different hours to create a daily dataset.  
    '''
    for year in sorted(os.listdir(input_dir)):
    # for year in ['2020']:        
        yearpath = os.path.join(input_dir, year)
        if os.path.isdir(yearpath):
            
            if config['Debug_Mode']:
                print('year')
                print(year)

            for month in sorted(os.listdir(yearpath)):
            # for month in ['11','12']:
                
                if config['Debug_Mode']:
                    print('month')
                    print(month)

                monthpath = os.path.join(yearpath, month)
                if os.path.isdir(monthpath):

                    if config['Debug_Mode']:
                        print('year')
                        print(year) 
                    
                    for day in sorted(os.listdir(monthpath)):
                    # for day in ['1','2']:
                        daypath = os.path.join(monthpath, day)
                        if os.path.isdir(daypath):

                            if config['Debug_Mode']:
                                print('day')
                                print(day)
                            
                            new_file_dir = os.path.join(output_dir, year, month)                # Define new file dir
                            os.makedirs(new_file_dir, exist_ok=True)                            # Make folders that do not exist
                            new_fileD = os.path.join(new_file_dir, day + '.pkl')                # Define file name

                            # if config['Debug_Mode']:
                                # print('sample_size')
                                # print(sample_size)

                            output_data = sc_tools.merge_dataset(config, daypath, keyword_list, sample_size )     # Merge all hourly files into day file, with stratified sampling
                            
                            if config['Debug_Mode']:  
                                print(daypath)
                                print(new_fileD) 
                            print(new_fileD) 

                            output_data.to_pickle(new_fileD)     

            