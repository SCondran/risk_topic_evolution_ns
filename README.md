Ctrl Shift v   - to open as markdown preview


# Quick steps #

- run step by step in 
```/apps/code/topic_analysis_complete_code_SC.ipynb```
note for this file some of the commentry is out of date, but all the code works
or 

- run in command line using .py 
## 1 Data processing ##
1. Download data and save as following
  - /Data_Path/{dataset}_data/1raw/
2. Run ```./2_process_data.py``` 
  -  Update paths 
  - Currently dataset is not in argpars so manually select platform and run for each 
  - Run ```./2b_merge_process_data.py``` if run step 2 with Parler data in parallel
3. Run ```./3_create_datasets.py``` 
  - Update paths 
    - Base_Path, Data_Path, Raw_Path, Processed_Path, {dataset}_Path, Avaliable_Path_Dict
  - Currently dataset is not in argpars so manually select platform and run for each 
## 2 Topic Analysis ##
1. Create event files
  - ``` ./4_convert_GSREvents.py``` 
2. Run analysis
  - Run  ```./5_topic_analysis.py``` 
    - Change threshold 






# Steps in depth #

## 1 Data processing ##
### 1. Import raw data ###

!! save data in ```./Data_Path/{dataset}_data/1raw/``` !!
Currently have 3 datasets 
a. Parler data random split of 2017-2021 into 166 files
  - https://doi.org/10.5281/zenodo.4442460
  - https://zenodo.org/record/4442460#.YuceVHZByUl 
  - Max Aliapoulios, Emmi Bevensee, Jeremy Blackburn, Barry Bradlyn, Emiliano De Cristofaro, Gianluca Stringhini, & Savvas Zannettou. (2021). A Large Open Dataset from the Parler Social Network (Version 1) [Data set]
b. Reddit saved per month, not chronological order
  - https://files.pushshift.io/reddit/comments/ 
c. Twitter saved in hourly files for each day, month, year
  - https://archive.org/details/twitterstream 




### 2. Process data ###
!!! working folder  ```./Data_Path/{dataset}_data/2format/``` !!

Step to process the raw data into cleaned data saved by year, month, day, hour.pkl

#### Read in variables ####
- Changes for ```2_process_data.py``` 
  - Paths
    - Base_Path
    - Data_Path
    - Processed_Path
    - Raw_Path --- this is were the GSREvents.csv and Event_Date_Data.csv exist
    - {dataset}_Path eg Reddit_Path, Parler_Path
    - Avaliable_Path_Dict

  - Define Platform
  - If making changes
  - If delete folders - Reddit, Parler process the data with appends, so need to delete existing folders before starting
  - If debug mode - this enables various prints througout the code
  - Check path exists - path is based on platform


- Reddit was run in parallel containers python not visual studio code. 
-note- To run as python script created new py file ```2_process_data.py``` changing the platform and various config variables before running. 
--note-- raw data in 1raw was split into different run folders (run?) to enable argparse to run muliple times 
in command line run  ```python filename.py -r run?  ```
---note--- process cannot be working on the same files at the same time ie cant run 2021/01 multiple times (this is mainly a concern for Parler data though, not Reddit)
To combat this 
``` config['output_dir'] = os.path.join(the_path, '2format')``` 
2format was manually changed to 2?format changing ? to a-z for each container. This however needs to be merged at the end using ```2b_merge_process_data.py```

#### Reddit ####
a. Read in whole file, loop through all date and hour
  ```sc_tasks.format_data_reddit```
  ```sc_tools.format_data_reddit2```
b. Format columns - remove columnns, rename, format date  
  ```sc_tools.extract_data_reddit```
c. Clean the data  
  ```sc_tools.cleaning_2```
    - make all text lower case
    - remove emoji
    - remove punctutation
    - remove non english words
    - spell check - was removed as took so long
    - lemmatize
    - remove stop words - also custom stop words
    - remove frequency words - while variable selected was bottom 20% and top 1%
    - drop empty rows
d. Add sentiment
  ```sc_tools.sentiCal3```
e. Save in hourly files

#### Parler ####
a. Read in whole file, loop through all date and hour
  ```sc_tasks.format_data_parler```
b. Format columns - remove columnns, rename, format date  
  ```sc_tools.extract_data_parler```
c. Clean the data  
  ```sc_tools.cleaning_2```
  - make all text lower case
  - remove emoji
  - remove punctutation
  - remove non english words
  - spell check - was removed as took so long
  - lemmatize
  - remove stop words - also custom stop words
  - remove frequency words - bottom 20% and top 1%
  - drop empty rows

d. Add sentiment
  ```sc_tools.sentiCal3```
e. Save in hourly files

#### Twitter ####
This has not been completed yet

## Create datasets ##


!!! working folder  ```./Data_Path/{dataset}_data/3combined/{eventname}/``` !!
!!! working folder  ```./Data_Path/{dataset}_data/4yearlycombined/{eventname}/``` !!
Step to get processed to individual datasets for each event saved by year
the ``` 2format ``` data is selected using keywords/hashtags sampled and saved in new folders


This was run in parallel using the command line
-note- To run as python script created new py file ```3_create_datasets.py``` changing the platform and eventname. 

a. Define config variables
b. Identify words associated with the event 
c. Define input and output directories
  - input_dir - ```2format``` 
  - output_dir - ```3combined/ {eventname}```
  - input_dirY - ```3combined/{eventname}```
  - ouput_dirY - ```4yearlycombined/{eventname}```
d. Create datasets for each data
  - ```sc_tasks.create_sampled_dataset_daily(config, input_dir, output_dir, keyword_list, sample_size=5000 )``` 
  For each folder in input_dir by year, month, day
    - Concat all files  
    - ``` sc_tools.merge_dataset```
      - If samplesize ``` sc_tools.sample_dataset```
        - Remove neutral sentiment - between -0.2 and 0.2
        - Keep rows where words within keyword_list exists
        - Stratified sampling by hour 
        - Return data
      - Return data
    - Save data in output_dir

e. Create dataset for each year 
```sc_tools.merge_daily_to_year_file(config, input_dirY, output_dirY) ```
- For all files in input_dirY by year 
- ``` sc_tools.merge_dataset```
  - Concat all files 
  - Return data
- Save data in ouput_dirY



## 2 Topic Analysis ##
!! save data in ```./Base_Path/models/OutputResults_batch/``` !!

### 2a create GSREvents ###
- Create GSREvents file 
  - ``` ./data/raw/GSREvents_{name}.csv ```
    - date,event,severity 
    - eg 23/06/2018,1,0
- Run ``` ./4_convert_GSREvents.py``` 
  - note this saves the file into  ``` ./data/processed/GSREvents_{name}.pkl ```
### Run topic analysis ###
- Create Event file
  - ``` ./data/raw/Event_Date_Data.csv ```
    - event,startDate,endDate
    - eg CapitalRiots,1/12/2020,28/02/2021

- for ``` 5_topic_analysis.py ``` 
  - Changes
    - If new datasets need to add its path and add to ```Avaliable_Path_Dict```
      - naming covention``` {dataset}_data ```
    - I had to hard code some event date overwrites since some dataset did not cover the entire event. If you add a new dataset and it is missing data for specific events then add onto this block of code
    - The ```threshold ```
      - this decides the threshold of a riskscore to be identified as an event
  - variables of note (you dont need to do anything)
    - ```csvlist```     - List of events along with start and end dates
    - ```gvFolderLoc1```- Folder where output files go
    - ```gsrEvSrc```    - This is the file that had the ground truthed data. note this is a pkl. create the file as csv and using ```sc_tools.convert_events_csv_to_pkl(Raw_Path1, Processed_Path1, file_name)``` to convert to pkl

  - Whats happening 
    a. Read in variables
    b. Loop through each event 
      i. Define more paths
      ii. Create a dataset based on the years of the event.
        -  Data is collected from ```4yearlycombined/{eventname}``` , which has filtered data basedon key words. The files are saved by the year, and events spanning muktiple years need to be merged. 
      iii. If no data for a date within the range provide list of missing dates and go to next loop (event)
      iv. If all dates cotain data run main_trendy.trendy
        - The output can be saved as a .pkl file. this means you can run the code and print out the output instantly rather than running it the model again. 

  - note that this has been run in the .ipynb file a block at a time. this means that figure can be displayed. I have only recently moved it to seperate .py files, and you cant display figures on the cammand line. 
    - I didnt get around to saving the figure or the performance values in seperate files. I was just reading them off the .ipynb file
- for ```main_trendy.trendy``` 
  - to calculate the daily score I use myNtopics=5, myNfeatures=100 in  ```src.features.build_features.dailyCalc``` 




# To do #
- biggest issue is how the data was cleaned. 
  -  ```sc_tools.cleaning_2```
    - spell check - was removed as took so long
      - need to look at whether spell check needs to be included and what is a more effeicient method
    - remove stop words - also custom stop words
      - look into the customised stop words. are they different for diffreent topics or platforms
    - remove frequency words - while variable selected was bottom 20% and top 1%  
      - #################### this is the issue here
      - currently remove words at an hourly level, the patterns are lost here
      - possibly look at removing words at a larger time interval or...
        - but in real world application you cannot analyse future data to find word frequencies.  
        - do i need to remove frequent words?
- Explore the threshold, what is the best value? can it make a variable which can be changed on the cmd line.
- currently have chosen 5 topics/day and 100 features. 
  - explore the best value for these
  - currently hard coded in, make a variable that follows through each function
- currently define paths for each .py file. need to create config file with all this data in and an argparse to select relevent dataset
- log files do not work for reddit data
- visualise.py normcast (line 84) this currently needs to be changed for each event and platform. to keep topic scores between 0-1.
- Threshold precisoin recall graph part is not work. It times out and need to restart Visual studio. currently only works with a static value. ```visualize.visualizeTrend -> .assess_performance```
- I think i have fixed this but need to run from scratch to double check. dicObj (```main_trendy.mainFunction```) keeps saving in ```models/OutputResults_batch``` not ```models/OutputResults_batch/GlobalVariables``` currently need to manually move them over. 
- didnt get around to including the reddit .xz files which cover 2018/01 - 2018/10
  - had issues unzipping and reading these files
  - will need an if statement reading file type then process the different ones. 

# Future ideas #
- severity is a variable within this code. I havent looked into this at all. But the input event data has this feature which is default to 0 
- Change topic analysis to allow for parallel running atm python deoesnt like iplot
- ```visulise.visualizeTrend ``` normcast. Currently hardcoded value. Look into making change depending on the output to keep topic socres between 0-1. cant make it based purley on topic as will lose relativity betweeen topics for the days. can do it per day as will loss trend through time. need some work around this. 
- Need optimisation of ```visulise.visualizeTrend ``` leadTimePredict, vnoOccurrence, vtweetrateCutOff. For each platform. Currently trial and error to get optimised riskscores. The optimal leadtime seems to change depending on teh time frame of the data.
- Decompose the riskscore to see what impact individual topicscores have. in some topics there is moderate flatline. this can be attributed to overlap of topicscores. Identification of the impact of individual or all bar one topics would help interigate riskscore
- Need to look into why cannot get Riskscore without an event existing. This becomes a detection task rather then prediction. We want to predict the risk of an event occuring without any ground truth. The use of logreg might be a good place to start.
- Can geolocation of data occur to improve identifaction of specific events rather than only using keywords or hashtags. This might help in the identification of smaller events which are covered by a major event in a different country. (But then will the application of this be at a country level and not all data at once?)
- I havent looked at the realTime processing only batch. I dont know what the differences in set up would be




