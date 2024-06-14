### This adds the path location where we have the src folders stored
import sys, os, argparse

##### imports #####
import time
import pandas as pd
from plotly import __version__
from plotly.offline import init_notebook_mode
init_notebook_mode(connected=True)

#### Local Imports ###
from src.models import main_trendy
from src.lib import sc_tasks
from src.lib import sc_tools

### Supress sklearn warnings ###
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

pd.options.mode.chained_assignment = None 


Base_Path = '/mnt/apps/risk_topic_evolution/code'                            # This is the path to code dir
Data_Path = '/mnt/data/risk_topic_evolution'            # This is the path to the data

sys.path.insert(0, Base_Path)
sys.path.insert(0, os.path.join(Base_Path, "src"))



##### 1. Read in variables #####
"""
### User variables ###
Platform: The social media platform 

config: Global config passed around the code
Make_Changes: IF false code is run without saving to file
Debug_Mode: This prints extra information when running code
Delete_output_folder: This deletes the output directory. 

### System variables ###
initial_time: Time code starts running
File paths: Define various paths
"""
##### User variables ######
### Social media platform ###
Platform = 'reddit'
# Platform = 'parler'
# Platform = 'twitter'

### define config variables ###
config = {}

### Make changes ###
# config['Make_Changes'] = True
config['Make_Changes'] = False

### Debug mode ###
config['Debug_Mode'] = True
# config['Debug_Mode'] = False

### Delete output folder ###
# config['Delete_output_folder'] = True
config['Delete_output_folder'] = False


##### System variables #####
config['initial_time'] = time.time()

### File path ###
Raw_Path       = os.path.join(Data_Path, "raw")
Processed_Path = os.path.join(Data_Path, "processed")
Twitter_Path   = os.path.join(Data_Path, "Twitter_data")
Reddit_Path    = os.path.join(Data_Path, "Reddit_data")
Parler_Path    = os.path.join(Data_Path, "Parler_data")
Avaliable_Path_Dict = {'twitter': Twitter_Path,
                        'reddit' : Reddit_Path,
                        'parler' : Parler_Path}

the_path = sc_tools.platform_path(Platform, Avaliable_Path_Dict)


### Data filders ###
Path_1raw= os.path.join(the_path, '1raw')
Path_2format= os.path.join(the_path, '2format')
Path_2formattemp= os.path.join(the_path, '2formattemp')
Path_3combined= os.path.join(the_path, '3combined')
Path_4yearlycombined= os.path.join(the_path, '4yearlycombined')


### Delete existing output files ###
if config['Delete_output_folder'] and Platform in ['parler', 'reddit']:
    if os.path.exists(config['output_dir']):
        # shutil.rmtree(config['output_dir'])
        print(Platform + 'files deleted - ' + config['output_dir'])



def main ():
    """
    Read in Event_Date_Data.csv 
    event, startDate, endDate  
    Note: Dates are the search window around the event
    Loop through each event 
    """

    ### Run model with new data ###
    csvlist = sc_tools.get_csv(os.path.join(Base_Path, 'data', 'raw','Event_Date_Data.csv'))

    for event_row in csvlist:
        eventname = event_row['event']
        startDate = sc_tools.convert_date(event_row['startDate'], 'dateslash')
        endDate   = sc_tools.convert_date(event_row['endDate'], 'dateslash')
        

        ### Change end dates depending when datasets ###
        if eventname == 'CapitalRiots' and Platform == 'parler':
            endDate = sc_tools.convert_date('2021-01-11', 'dateline')
            print('---Change Parler CapitalRiots end date---')

        elif eventname == 'Brexit' and Platform == 'reddit':
            startDate = sc_tools.convert_date('2018-10-01', 'dateline')
            print('---Change reddit Brexit end date---')

        elif eventname == 'Brexit' and Platform == 'parler':
            startDate = sc_tools.convert_date('2019-01-01', 'dateline')
            print('---Change parler Brexit end date---')


        print(eventname)
        print(startDate)
        print(endDate)


        ### Paths and files ###
        analysisTitle = Platform + '_' + eventname + '_batchTopicAnalysis'   
        gsrEvSrc  = os.path.join(Base_Path, 'data', 'processed', 'GSREvents_' + eventname + '.pkl') 
        gvFolderLoc1  = os.path.join(Base_Path, "models", "OutputResults_batch" )
        fileout_name = os.path.join(Platform + '_' + eventname + '_' + str(startDate) + '_' + str(endDate) + '.pkl')       # Name of output file
        
        ### Get list of avaliable datasets ###
        yearly_data_path = os.path.join(Path_4yearlycombined, eventname)    
        yearly_datasets_avaliable = [os.path.splitext(filename)[0] for filename in os.listdir(yearly_data_path)]

        ### Append required yearly datasets ###
        allowed_dates = [y for y in range(startDate.year, endDate.year + 1)]
        temp_data = []
        for yearloop in allowed_dates:
                    
            year_file = os.path.join(yearly_data_path,  str(yearloop) + '.pkl')                                         # ID yearly file to use
            print(year_file)
            newDF = sc_tools.get_pkl(year_file)
            temp_data.append(newDF)        

        df = pd.concat(temp_data, ignore_index=True)


        ### Identify missing dates within the dataset ###
        missing_dates = pd.date_range(start=startDate, end=endDate).difference(df['publicationDate'])                   # Identify any dates that are missing from the range
        
        ### Interigation of the inputdata ###
        delta = endDate  - startDate 
        daysused = df.loc[(df['publicationDate']>startDate)  & (df['publicationDate'] <= endDate)]
        print('number of days: ' + str(round(delta.days + 1,0)))
        print('Number of rows in data: ' + str(len(daysused)))
        print('Average number of text per day: ' + str(round(len(daysused) / (delta.days + 1), 0)))
        print('Av text length: ' + str(round(daysused['text'].apply(len).mean(),0)))


        ### Flag on whether to run analysis or load from file ### 
        preload_dailycal = True

        if list(missing_dates):                                                                                         # If any dates identified as mssing then use this statement
            print('---- Error missing date ----') 
            print(missing_dates)                                                                                        # Print dates that are missing


        elif preload_dailycal:

            ### Define threshold of riskscore which corresponds to an event ###
            threshold = 0.4
            ### Use this list to get a precisoin/recall plot to find best threshold ###
            ## note this is currently not working
            # step = 0.1
            # threshold = list(np.arange(0, 1 + step, step))

            if os.path.exists(os.path.join(gvFolderLoc1, 'GlobalVariables', fileout_name)):            # if file topic file does exist
                print('\n\n\n##### Code running - Preload=True, Update=True #####')
                gvFolderLoc1  = os.path.join(Base_Path, "models", "OutputResults_batch" )
                trend_ret = main_trendy.trendy(dataSource=df, processing="batch", updating=True, startDate=startDate, endDate=endDate,
                            gsrEvents=gsrEvSrc, analysisTitle=analysisTitle, fileout_name=fileout_name, visualize=True, 
                            FolderLoc=gvFolderLoc1, preload_dailycal=True, threshold=threshold)

                if trend_ret == None:                   # If output returns none due to failure continue to next loop item
                    continue
            

            else:                                       # If topic file does not exist run with update=False 
                print('\n\n\n##### Code running - Preload=True, Update=False #####')
                trend_ret = main_trendy.trendy(dataSource=df, processing="batch", updating=False, startDate=startDate, endDate=endDate,
                                gsrEvents=gsrEvSrc, analysisTitle=analysisTitle, fileout_name=fileout_name, visualize=True, 
                                FolderLoc=gvFolderLoc1, preload_dailycal=False)
                
                if trend_ret == None:                   # If output returns none due to failure continue to next loop item
                    continue            
            

        else:                                           # This will run when preload=False                                                                                                
            ### Run model ###
            print('\n\n\n##### Code running - Preload=False #####')
            trend_ret = main_trendy.trendy(dataSource=df, processing="batch", updating=True, startDate=startDate, endDate=endDate,
                                gsrEvents=gsrEvSrc, analysisTitle=analysisTitle, fileout_name=fileout_name, visualize=True, 
                                FolderLoc=gvFolderLoc1, preload_dailycal=False)

            if trend_ret == None:                        # If output returns none due to failure continue to next loop item
                continue




if __name__ == '__main__':
    main()