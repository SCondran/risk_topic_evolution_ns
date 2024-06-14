# This is the main class
#__________________________________
#Classical Imports needed
from fileinput import filename
import pandas as pd
import time
import os
import pickle
import datetime
from threading import Timer
from IPython.display import display, clear_output
#____________________________________
#Imports from local src
import src.features.build_features as bf ## For defined functions
import src.features.build_globalVariables as gv ## For global variables
import src.visualization.visualize as vis ## For global variables
#from src.models import train_model as trM## For global variables


import src.lib.sc_tools as sc_tools

def trendy(dataSource=None, processing="batch", updating=True, startDate=None, 
            endDate=None, gsrEvents=None, analysisTitle=None, fileout_name=False,
            visualize=False, FolderLoc=None, frequency="daily", preload_dailycal=False, threshold=0.6):
    """
    This function is the de-facto main function. It takes as input the following parameters, discovers the
    topics, calculates and stores the scores. It may or may not return a visualisation depending on the 
    specified parameters.
    --------------------------------------------------------------------------------------
    :param dataSource (default - None): This is the .pkl file location of the processed data (as pandas 
    dataframe) containing the columns text, sentiment and date. It can contain other columns but these 
    three are REQUIRED. Default value None, this will throw an exception - relating to no data to process.

    :param processing (default - 'batch'): Determines whether the dataset will be analysed as a set wholly 
    or in a real time sequence of the dates (or times). Options are:
        - batch - This will analyse all the tweets in the period - good for explorations of historic data
        - realTime - This is for realtime analysis. The frequency of the analysis is specified by the
        param "frequency".

    :param updating (default -True): Determines whether the analysis is updating a stored topic model or
    creating a new topic model from scratch. Options are: 
        - True - it will load an existing model called the param "analysisTitle" from the param "FolderLoc"
        and update it.
        - False - it will create a model from scratch and store it as the param "analysisTitle" in the 
        param "FolderLoc"
   
    :params startDate/endDate (defaults - None): This is date to start and end the analysis on the 
    dataset given. In "batch" processing, both are REQUIRED. In "realTime" processing endDate is not required.
    If realTime analysis is to start from a day prior to the current day, then startDate is required.
    Note that the date must have time if a 'frequency' hourly realtime analysis is to be done.

    :param gsrEvents (default - None): Gives the location where the recorded gsr events are stored. 
    This is a dictionary of the form {date_1:[x,y],....,date_n:[x,y]}. Date is the date that event occurs, 
    x is a binary value whether or not an event occured and y is a value that represents the intensity of 
    the event on a scale between 0 and 1 (not being used atm - but can be used in the prediction).If 
    default None, then probabilty prediction (logistic regression) cannot be made - obviously no ground Truth.
    
    :param analysisTitle (default - None): This the name of the analysis being done eg. FreeportData etc. 
    This becomes part of the file name that will hold the results. It is the same file name if existing
    results are to be used.
    
    :param FolderLoc (default - None): This the folder were all the results are stored - should be given
    a meaningful name. Results include the topicmodels and the global variables used. The topic model file 
    will be stored within a special folder called "TopicResults" containing only topic files. The global 
    variables include the topicfeaturemap,theTopicNo, som model, and globalTopics. Each is stored in the
    special folder called GlobalVariables.
    
    :param visualize (default - False) : This decides whether you want to visualize your results after
    discovering the topics.
    
    :param frequency (default - 'daily') : This the frequecy with which the data analysis will be done 
    in the realTime processing. Options are "daily" (default) or "hourly". 
    KNOWN ISSUES: If frequency is less than 1 hour eg. having minutes brings all sorts of complications 
    for instance matching the tweets timestamps to the minutes (frequency) in dateList. Also it must be 
    ensured that the operations can be perfomed in less than the specified number of minutes. In principle
    this is not impossible - but I leave it to the engineers.
    """
    if FolderLoc is None: 
        gvFolderLoc = None 
        # T.1. Above is not necessary. It is a place holder to create a folder if there's none 
    else:
        gvFolderLoc = os.path.join(FolderLoc, "GlobalVariables")

    if analysisTitle is None:
        topicsLoc = None 
        #T.2. Similar to above comment T.1
    else: 
        topicsLoc = os.path.join(FolderLoc, "TopicResults", analysisTitle + ".pkl")
    
    global dicObj 
    dicObj = dict()
    start_time = time.time()


    #***************************************************************
    #..............batch processing........................
    #****************************************************************
    if processing == "batch":

        main_ret = mainFunction(mdataSource=dataSource, mprocessing=processing, mupdating=updating,
                     mstartDate=startDate, mendDate=endDate, mgsrEvents=gsrEvents, mtopicsLoc=topicsLoc,
                     mvisualize=visualize, mgvFolderLoc=gvFolderLoc, mfrequency=frequency, 
                     preload_dailycal=preload_dailycal, fileout_name=fileout_name, threshold=threshold)


        if main_ret == None:
            return

        print("--- %s seconds ---" % (time.time() - start_time)) 


        
    #***************************************************************
    #................realtime processing.........................
    #****************************************************************
    elif processing == "realTime":
        '''
        KnownIssues: If for some reason, there is no data on a given day (or hour) no results will be 
        returned. If you attempt to restart the analysis from a time where there was results, you need 
        to change the attribute "updating" in initial run of the main function to True, otherwise the 
        existing results will be replaced.
        '''

        #************Local function for recursive calling of MainFunction************
        def realTimer(dataSource, processing, updating, startDate, endDate,
                      gsrEvents, topicsLoc, visualize, gvFolderLoc, frequency, secs):
            '''
            This is a local functions that allows us to call the mainfunction recursively every 'secs' secs.
            --------------------------------------------------------------------------------
            params : all other params are as defined in the function mainFunction.
            '''
            clear_output( wait = True)
            main_ret = mainFunction(mdataSource=dataSource, mprocessing=processing, mupdating=True, mstartDate=startDate,
                         mendDate=datetime.datetime.today().replace(microsecond=0,second=0,minute=0), 
                         mgsrEvents=gsrEvents, mtopicsLoc=topicsLoc, mvisualize=visualize, 
                         mgvFolderLoc=gvFolderLoc, mfrequency=frequency)

            if main_ret == None:
                return

            Timer(secs, realTimer, args=(dataSource, processing, updating, startDate, endDate, gsrEvents,
                                          topicsLoc, visualize,  gvFolderLoc, frequency, secs)).start()
        #*******************************************************************************************
        
        if startDate is not None:
            if frequency == "daily":
                endDate = datetime.datetime.today().replace(microsecond=0,second=0,minute=0)-datetime.timedelta(days=1)

            else:  #T.3. Else this is hourly
                endDate = datetime.datetime.today().replace(microsecond=0,second=0,minute=0)-datetime.timedelta(hours=1)
            main_ret = mainFunction(mdataSource=dataSource, mprocessing=processing, mupdating=False,
                         mstartDate=startDate, mendDate=endDate, mgsrEvents=gsrEvents, 
                         mtopicsLoc=topicsLoc, mvisualize=False, mgvFolderLoc=gvFolderLoc,
                         mfrequency=frequency)

            if main_ret == None:
                return
 
            xStart= datetime.datetime.today().replace(microsecond=0, second=0, minute=0)
            if frequency == "daily":
                yEnd = xStart.replace(day=xStart.day+1, hour=1, minute=0, second=0, microsecond=0)
                
            else:  #T.4. frequency is "hourly":
                yEnd = xStart.replace(day=xStart.day, hour=xStart.hour+1, minute=0, second=0, microsecond=0)
            
            delta_t = yEnd - xStart
            secs = delta_t.seconds + 1
            realTimer(dataSource, processing, True, startDate, 
                      datetime.datetime.today().replace(microsecond=0, second=0, minute=0), 
                      gsrEvents, topicsLoc, visualize, gvFolderLoc, frequency, secs)


        else:  #T.5. This handles the case where start date not specified 
            if frequency == "daily":
                startDate = datetime.datetime.today().replace(microsecond=0,second=0,minute=0)- datetime.timedelta(days=1)
                endDate   = datetime.datetime.today().replace(microsecond=0,second=0,minute=0)- datetime.timedelta(days=1)
            else: #T.6. frequency is "hourly":
                startDate = datetime.datetime.today().replace(microsecond=0,second=0,minute=0)-datetime.timedelta(hours=1)
                endDate   = datetime.datetime.today().replace(microsecond=0,second=0,minute=0)-datetime.timedelta(hours=1)
            
            main_ret = mainFunction(mdataSource=dataSource, mprocessing=processing, mupdating=False,
                         mstartDate=startDate, mendDate=endDate,
                         mgsrEvents=gsrEvents, mtopicsLoc=topicsLoc, mvisualize=False, 
                         mgvFolderLoc=gvFolderLoc, mfrequency=frequency)


            if main_ret == None:
                return

            xStart = datetime.datetime.today().replace(microsecond=0, second=0, minute=0)
            
            if frequency == "daily":
                yEnd = xStart.replace(day=xStart.day+1, hour=1, minute=0, second=0, microsecond=0)
                
            else:  #T.7. frequency is "hourly":
                yEnd = xStart.replace(day=xStart.day, hour=xStart.hour+1, minute=0, second=0, microsecond=0)
            
            delta_t = yEnd - xStart
            secs = delta_t.seconds + 1
            
                
            realTimer(dataSource, processing, True, startDate,
                      datetime.datetime.today().replace(microsecond=0, second=0, minute=0), 
                      gsrEvents, topicsLoc, visualize, gvFolderLoc, frequency, secs)
        
        print("--- %s seconds ---" % (time.time() - start_time))
        



   

    #***************************************************************
    #..................No processing specified.......................
    #****************************************************************  
    else: 
        print ("No processing type specified")
        print("--- %s seconds ---" % (time.time() - start_time))    
    




#***************************************************************
#.....................mainFunction.............................
#****************************************************************
def mainFunction(mdataSource=None, mprocessing="batch", mupdating=True, mstartDate=None, mendDate=None, 
                mgsrEvents=None, mtopicsLoc=None, mvisualize=False, mgvFolderLoc=None, mfrequency="daily", 
                preload_dailycal=False, fileout_name=False, threshold=0.6):
    """
    This is the actual main function that does all the operations needed. It can be called separately or it
    can be called via Trendy. At the moment, it is configured to be called via trendy. It is this way because
    I want to be able to recursively call this function multiple times withhin trendy.*** This can be
    simplified by putting all the code here inside of trendy and making the recursive call within  trendy
    itself. For now I leave it this way.
    -----------------------------------------------------------------------------------------------------
    params: As defined in trendy, except preceded with 'm' and with the following exceptions:
            mtopicsLoc: This is the actual path of the file with the topics.
            mgvFolderLoc: This is the actual path to the folder containing the global variables.
    """
    if mupdating:
        gv.updateGV(mgvFolderLoc)
        print("Preload updateGV")
        storeddicObj = bf.getData_2(mtopicsLoc)

        # if mfrequency == "daily":
        #     mstartDate = sorted(list(storeddicObj.keys()))[-1] + datetime.timedelta(days=1)
        # else: #MF.1. This is hourly
        #     mstartDate = sorted(list(storeddicObj.keys()))[-1] + datetime.timedelta(hours=1)
    
    else:
        print("New updateGV")
        gv.initGV()
        storeddicObj = None
        
    dateMap = dict() 
    # indo_EngTweetsPd_sent = bf.getData_2(mdataSource)
    indo_EngTweetsPd_sent = mdataSource
    indo_EngTweetsPd_sent = indo_EngTweetsPd_sent[(indo_EngTweetsPd_sent.publicationDate>=mstartDate)&\
    (indo_EngTweetsPd_sent.publicationDate<=mendDate)]
    
    headerList = list(indo_EngTweetsPd_sent.columns.values)
    headerMap = dict()
    i=1 
    #MF.2. We start from 1 because there is always an unlabled row number column in the pandas dataframe
    for each in headerList:
        headerMap[each] = i
        i=i+1

    dateList = []
    
    if mfrequency == "daily":
        for single_date in bf.daterange(mstartDate, mendDate+ datetime.timedelta(days=1), interval = mfrequency):
            dateList.append(single_date)
    else: # MF.3. frequency is "hourly"
        for single_date in bf.daterange(mstartDate, mendDate+ datetime.timedelta(hours=1), interval = mfrequency):
            dateList.append(single_date)
    


    # filetosave = os.path.join(mgvFolderLoc, "outputdailycal.pkl")




    ### Run dailycal and save to file ###
    if not preload_dailycal:
        filetosave = os.path.join(mgvFolderLoc, fileout_name)
        print(filetosave)
        newDicObj = bf.dailyCalc(indo_EngTweetsPd_sent, dateList, headerMap, myNtopics=5, myNfeatures=100, 
                                dicObj=storeddicObj, bfgvFolderLoc=mgvFolderLoc)
        dicObj = newDicObj

        for key in sorted(dateList):
            # print(key)
            for tkey in dicObj[key].keys():
                dicObj[key][tkey].updateObj(tkey,dicObj, dateList, window=1) 
                #MF.4. Above calculates the scores
                dicObj[key][tkey].updateSummary(tkey, n_top_words=10)
                #MF.5. Above collects the values together as a string and stores it within the object as a summary
                        # Save all data to file
        # print(dicObj)
        with open(filetosave, "wb") as f:
            pickle.dump(dicObj, f)

        
    ### Read existing dailycal data ###
    ### Note: file must already have been created ###
    else:
        
        filetosave = os.path.join(mgvFolderLoc, fileout_name)
        # print(filetosave)
        from pathlib import Path

        if not os.path.isfile(filetosave):
            print('--- Error--- does not exists' + str(filetosave))
            return
        
    
        print("Preloading dailyCal data")          
        newDicObj = sc_tools.get_pkl(filetosave)
        dicObj = newDicObj


    
    # print(dicObj)
    thedict = dicObj

    #MF.6. Stores (updates) the topic dictionary object in 'mtopicsLoc'  for future use
    with open(mtopicsLoc, "wb") as f:
        pickle.dump(thedict, f)
    
    #********************The following reads in the GSR events if they exist***********************
    
    if mgsrEvents:
        GSREvents=bf.getData_2(mgsrEvents)
        bf.GSREventRead(GSREvents, dateList)

    #******************************************************************************************
    
    with open(os.path.join(mgvFolderLoc, "globalTopics.pkl"),"wb") as f:
        pickle.dump(gv.globalTopics, f)

    with open(os.path.join(mgvFolderLoc, "theTopicNo.pkl"),"wb") as f:
        pickle.dump(gv.theTopicNo, f)

    # print("+++++++++++++++++++++++++++ topicNo")
    with open(os.path.join(mgvFolderLoc, "topicFeatureMap.pkl"),"wb") as f:
        pickle.dump(gv.topicFeatureMap, f)

    
    #MF.7. The above stores (updates) the global variables in respective files for future use



    
    #**************************************************************************************
    #...................................Visualisation.......................................
    #**************************************************************************************
    if mvisualize:
        vis.visualizeTrend(vDicObj=dicObj, datelist=dateList, vGsrEvents=GSREvents, vfrequency=mfrequency, threshold=threshold)

    


    return dicObj




    
    
    
    
    
    
    
        