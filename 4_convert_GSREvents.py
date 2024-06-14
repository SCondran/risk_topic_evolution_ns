import os

from src.lib import sc_tasks
from src.lib import sc_tools

Base_Path = '/mnt/apps/risk_topic_evolution/code'                            # This is the path to code dir
# Data_Path = '/mnt/data/risk_topic_evolution'            # This is the path to the data



##### Convert GSREvents csv to pkl #####
'''
Manual step to make GSREvents
Convert date of event with event occuring and risk of event to dictionary and save as pkl
date          event        severity
dd/mm/yyyy    1            0
type(date)    type(int)    type(int)

Note: Each event has its own file
'''

Raw_Path1 = os.path.join(Base_Path, 'data', 'raw')
Processed_Path1 = os.path.join(Base_Path, 'data', 'processed')

### Option1 - Split groundtruth events by event group. This is to calculate classification performance ### 
event = 'Brexit'
file_name = 'GSREvents_'  + event

### Option2 - All civil unrest events ###
# file_name = 'GSREvents'

sc_tools.convert_events_csv_to_pkl(Raw_Path1, Processed_Path1, file_name)