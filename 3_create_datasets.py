
### imports ###
import sys, os, argparse
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import pandas as pd
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


# Raw_Path       = os.path.join(Base_Path, "data", "raw")
Processed_Path = os.path.join(Data_Path, "processed")
Twitter_Path   = os.path.join(Data_Path, "Twitter_data")
Reddit_Path    = os.path.join(Data_Path, "Reddit_data")
Parler_Path    = os.path.join(Data_Path, "Parler_data")
Avaliable_Path_Dict = {'twitter': Twitter_Path,
                        'reddit' : Reddit_Path,
                        'parler' : Parler_Path}

sys.path.insert(0, Base_Path)
sys.path.insert(0, os.path.join(Base_Path, "src"))


### Step 3: Merge datasets ### 
''' 
With the clean data saved in hourly files. 
Create a dataset in a specific time frame and covering a relevent topic (if known)
'''

######################################################################
######################################################################
######################################################################
''' 
This part is a bit of a hack
There was a lot of posts for each platform. If there are to many on a specific day the code errors out (not sure if this is computing power?)
I needed to combine a subset of each hour into a daily dataset (then one for the time frame of an event)
So I identified keywords form news articles and hashtags to select posts with this words
These key words helped create a more topic relevent dataset - the correct way of this I would assume is the use of hashtags or something....
I originally had the code using this in a different .py file and ran it multiple times changing the 'eventname'
I need to create a file with each of these eventname and key words and loop through this file to run the relevent lies of code
'''

### Social media platform ###
# Platform = 'reddit'
Platform = 'parler'
# Platform = 'twitter'


### define config variables ###
config = {}
config['initial_time'] = time.time()

### Make changes ###
config['Make_Changes'] = True
# config['Make_Changes'] = False

### Delete output folder
# config['Delete_output_folder'] = True
config['Delete_output_folder'] = False

### Debug mode ###
# config['Debug_Mode'] = True
config['Debug_Mode'] = False

### Delete existing Parler files ###
if config['Delete_output_folder'] and Platform in ['parler', 'reddit']:
    if os.path.exists(config['output_dir']):
        # shutil.rmtree(config['output_dir'])
        print(Platform + 'files deleted - ' + config['output_dir'])


###################################
eventname = 'BlackLivesMatter'
###################################

if eventname == 'CapitalRiots':
    ### CapitalRiots ###
    keyword_list = ['white', 'riot', 'fraud', 'freedom', 'election', 'country', 'wwg1wga', 'biden', 'trump', 'office', 'president',
                    'pedophile', 'conspiracy', 'better', 'great', 'again', 'democrat', 'true', 'evil', 'traitor']                                                       # Add words as required 

if eventname == 'Brexit':
### Brexit 1/5/2018 - 31-12/2019 ###
    keyword_list = ['brexit', 'referndum', 'EU', 'vote', 'time', 'labour', 'country', 'parliament', 'britain', 'party', 
                    'ireland', 'tory', 'shambles', 'deal', 'stop', 'post', 'hard', 'today']

if eventname == 'HongKongProtests':
### HongKongProtests  1/2/2019 - 30/7/2019 ###
    keyword_list = ['anti', 'extradition', 'law', 'admendment', 'bill', 'movement', 'hong', 'kong', 'protest', 'autonomous', 
                    'one', 'country', 'two', 'system', 'capitalist', 'economy', 'china', 'bejing', 'legislative', 'boycott', 
                    'decentralise', 'decentralize', 'chinese', 'umbrella', 'movement']

if eventname == 'Indonesia':
### Indonesia 1/8/2019 - 31/12/2019 ###
    keyword_list = ['extramarital', 'sex', 'defamination', 'president', 'corruption', 'president', 'kendari', 'student', 'violence',
                    'bill', 'mining', 'land', 'labour', 'correctional', 'rkuhp', 'ruu kpk', 'ruu pks', 'revise', 'investigate', 
                    'eradication', 'commision', 'law']

if eventname == 'BlackLivesMatter':
### BlackLivesMatter 1/4/2020 - 31/12/2020 ###
    keyword_list = ['racisim', 'police', 'brutality', 'violence', 'BLM', 'George', 'Floyd', 'Trayvon', 'Martin', 'defund', 'murder', 
                    'Derek', 'Chauvin', 'I', 'cant', 'breathe', 'second', 'degree', 'dont', 'shoot', 'brother', 'blue', 'commitment',
                    'knee', 'neck', 'off', 'reform', 'black', 'lives', 'matter']  

if eventname == 'Lebanon':
### Lebanon 1/8/2019 - 31/12/2019 ###
    keyword_list = ['tax', 'gasoline', 'tobacco', 'sectarian', 'rule', 'stagnation', 'economy', 'unemployment', 'endemic', 'corruption', 
                    'beruit', 'explosion',  'red', 'october', 'revolution', 'rule', 'governemnt', 'prime', 'minister', 'saad', 'hariri', 
                    'resign', '72 hours']

### Define file path ###
the_path = sc_tools.platform_path(Platform, Avaliable_Path_Dict)
# input_dir = os.path.join(the_path, '2format')
input_dir = os.path.join(the_path, '2format')
output_dir = os.path.join(the_path, '3combined',eventname)

input_dirY = os.path.join(the_path, '3combined', eventname)
output_dirY = os.path.join(the_path, '4yearlycombined', eventname)

### Merge houlry datasets into daily, crating subsample ###
## note have changed to do only 2020,2021 (but 2 months not don yet)
sc_tasks.create_sampled_dataset_daily(config, input_dir, output_dir, keyword_list, sample_size=5000 )

### Create yearly dataset, merge each day  ###
sc_tools.merge_daily_to_year_file(config, input_dirY, output_dirY)

