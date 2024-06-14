
### imports ###
import sys, os, argparse
from datetime import timedelta, date
import datetime
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import colorlover as cl
import plotly as py  
import plotly.graph_objs
import math
import json
import numpy as np
import pickle as pkl
import pandas as pd
import csv
import zstandard
import shutil
import time


from src.lib import sc_tasks
from src.lib import sc_tools

# supress sklearn warnings
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

pd.options.mode.chained_assignment = None 

### This adds the path location where we have the src folders stored
Base_Path = '/mnt/apps/risk_topic_evolution/code'                            # This is the path to code dir
Data_Path = '/mnt/data/risk_topic_evolution'            # This is the path to the data

Raw_Path       = os.path.join(Base_Path, "data", "raw")
Processed_Path = os.path.join(Data_Path, "processed")
Twitter_Path   = os.path.join(Data_Path, "Twitter_data")
Reddit_Path    = os.path.join(Data_Path, "Reddit_data")
Parler_Path    = os.path.join(Data_Path, "Parler_data")
Avaliable_Path_Dict = {'twitter': Twitter_Path,
                        'reddit' : Reddit_Path,
                        'parler' : Parler_Path}

sys.path.insert(0, Base_Path)
sys.path.insert(0, os.path.join(Base_Path, "src"))






### Input Vars ###
parser = argparse.ArgumentParser(description='ArgParse', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-r', '--runfolder', default="", help='runfolder')
args = parser.parse_args()
#### 


### Social media platform ###
Platform = 'reddit'
# Platform = 'parler'
# Platform = 'twitter'

### define config variables ###
config = {}
config['initial_time'] = time.time()

### Make changes - This manages whether the code saves files or runs the code and prints out. If no files saving check here ###
# config['Make_Changes'] = True
config['Make_Changes'] = False

### Delete output folder - You can delete all out puts and start from scratch here. Make sure you want to delete it ###
# config['Delete_output_folder'] = True
config['Delete_output_folder'] = False

### Debug mode  - This prints various parts of the code ###
config['Debug_Mode'] = True
# config['Debug_Mode'] = False

### Delete existing Parler files - There is a bug somewhere and needs to delete the Parler output files before starting. ###
if config['Delete_output_folder'] and Platform in ['parler', 'reddit']:
    if os.path.exists(config['output_dir']):
        # shutil.rmtree(config['output_dir'])
        print(Platform + 'files deleted - ' + config['output_dir'])

### Input Vars ###
parser = argparse.ArgumentParser(description='ArgParse', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-r', '--runfolder', default="", help='runfolder')
args = parser.parse_args()

### Define file path ###
'''
1raw - intial raw data - need to create folder and copy raw data into here !!!
2format - Data in same format is saved here. in folders /year/month/day/hr.pkl . raw data from diffrent platforms is in different formats, 
3clean - Clean data is saved here - this removes emoji, puctuation stop word etc. 
'''
the_path = sc_tools.platform_path(Platform, Avaliable_Path_Dict)
config["input_dir"] = os.path.join(the_path, '1raw', args.runfolder)
config['output_dir'] = os.path.join(the_path, '2format')



def main ():

    ### Step 2: Read in raw data and save formated clean data ###
    loop_number = 0
    for full_file_name in sorted(os.listdir(config['input_dir'])):
        loop_start_time = time.time()
        print(full_file_name)
        if Platform == 'reddit':
            sc_tasks.format_data_reddit(config, full_file_name, args)

        elif Platform == 'parler':
            sc_tasks.format_data_parler(config, full_file_name, args)
            pass



        loop_number += 1
        print("  Loop Time: " + str(round(time.time() - loop_start_time)) + "s\n")

    print("\nTotal Time: " + str(round(time.time() - config['initial_time'])) + "s\n\n")


if __name__ == '__main__':
    main()
