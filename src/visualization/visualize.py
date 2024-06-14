#classical Imports
#import plotly.plotly as py #replaced by below
import plotly as py #replaced by below
import plotly.graph_objs
from datetime import timedelta, date
from plotly.graph_objs import Scatter, Figure, Layout, Bar #replaced by below
#from chart_studio.plotly.graph_objs import Scatter, Figure, Layout, Bar
import colorlover as cl
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot #replaced by below
#from chart_studio.plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

import datetime
import numpy as np
import pandas as pd

#local Imports
#import src.features.build_features as bf
import src.models.train_model as  trM## For global variables
import src.features.build_features as bf## For defined functions
import src.features.build_class as bc## for defined classes
import src.features.build_globalVariables as  gv

bupu = cl.scales['9']['seq']['BuPu']
bupu500 = cl.interp( bupu, 1000 )

def visualizeTrend(vDicObj =None, sourceDicObj=None, vGsrEvents=None, sourceGSR=None, datelist=None,
                    vsearchWindow=None,vnoOccurrence=2,

                    vTweetRateCutOff=20, leadTimePredict=9, vfrequency='daily', threshold=0.6):            #I have changed the vTweetRateCutOff Default from 10 to 1 and vnoOccurrence from 2 to 1

    """
    This funtion plots a graph with the following:
    (1) A bar graph that indicates the presence or absence of a GSR Event
    (2) A calculated score trend line for each topic dicovered for each day
    (3) An overall trend line (sum of all topic scores) for all topics for each day
    (4) A probability Score for the occurence of an event in the next window days eg 3 days
    
    KnownIssues: Plottly seems to convert time to GMT so if you do a realTime analysis with
    an hourly frequency, the results (dictionary) are stored in local time but the display 
    is converted to GMT. 
    -----------------------------------------------------------------------------------------
    param vDicObj: This is the dictionary of topics. This is either passed in by the calling function such as
    trendy. Otherwise it is none. In which case a source where this Topic dictionary file is stored must be
    specified in the param "sourceDicObj"
    
    param sourceDicObj: This is the location of the pkl file that has the dictionary objects. We need to
    specify this if the topic dictionary has not been passed in.Eg. if we are only interested to visualise
    an already discovered topic distionary.
    
    param vGsrEvents: This is the dictionary of GSR events. This is either passed in by the calling function
    such as trendy. Otherwise it is none. In which case a source where this GSR dictionary file is stored 
    must be specified in the param "sourceGSR"
    
    param sourceGSR: This is the location of the pkl file that has the dictionary of GSR events. We need to
    specify this if vGsrEvent has not been passed in.Eg. if we are only interested to visualise
    an already discovered topic distionary. If this value does not exist then it implies there is no GSR data
    to do the logistic regression.
    
    param datelist: This can be specified so that the graphing will consider only a certain period. The list
    must be a list of valid dates. If nothing is specified then the function will use the dates of the topic
    ditionary.
    
    param vsearchWindow: This para determines the number of consecutive days within which to search for 
    an occurenc of the same topic. This is used when we filtere spurious topics.
    
    param vnoOccurrence: This para detemines the minimum cutoff threshold below which a topic is considered
    a spurious topic. eg a topic that appears less than vnoOccurence=2 times is considered spurious.
    
    param  vtweetrateCutOff: This para determines the minimum suupport a topic requires to be considered a
    valid topic. Eg a topic with less than vtweetrateCutOff= 10 tweets will be considered unsupported enough
    and filtered off.
    
    param leadTimePredict: This para determines what lead time to use when predicting the occurence of an 
    event. eg a leadTimePredict= 3  means that whatever the probaility of an event occuring given a specific
    day will be the probability that an event occurs within 3 days starting from that specific day.
    
    NOTE: line 193 is a switch for allowing us to see topic trend lines
    """

 
    global normConst
    # normConst = 25 
    normConst = 21

    print('leadTimePredict: ' + str(leadTimePredict) + '  - vnoOccurrence: ' + str(vnoOccurrence) +
          '  - vtweetrateCutOff: ' + str(vTweetRateCutOff) + '  - normConst: ' + str(normConst))



    #Vis.1. Above is moderation constant to reduce the score to between 0 and 1 (well not exactly).
    Terminate = False
    noProbabilityCalc = False 
    if vDicObj is None and sourceDicObj is not None:
        vDicObj =bf.getData_2(sourceDicObj)
    elif vDicObj is None and sourceDicObj is  None: 
        print("One of dicObj or sourceDicObj must be specified...Terminating...")
        Terminate = True
        #Vis.2. Exception above can be handled more gracefully - I leave this for now
    if vGsrEvents is None and sourceGSR is not None:
        vGsrEvents =bf.getData_2(sourceGSR)
    elif vGsrEvents is None and sourceGSR is  None: 
        print("The probability cannot be calculated since there is no GSR training data")
        noProbabilityCalc = True
        #Vis.3. Exception above can be handled more gracefully - I leave this for now
    
    if not Terminate:
        if datelist:
            datelists = sorted(datelist)
           
        else:
            datelists = sorted(list(vDicObj.keys()))
        
        if not noProbabilityCalc:
            bf.GSREventRead(vGsrEvents, datelists)
        
        vDicObj = {curKey:vDicObj[curKey] for curKey in datelists}

        filteredTopics = bf.filterSpurious(dicObj=vDicObj,searchWindow=vsearchWindow,
                                           noOccurrence=vnoOccurrence)
        y5,yth,Ally,Alltxt =  prepTopicVisual(filtered=filteredTopics,TRCutOff = vTweetRateCutOff,
                                              GSREvents=vGsrEvents)

        # print(datelists)
        # print('2+++++y5')
        # print(y5)

        # print('2+++++yth')
        # print(yth)

        # print('3+++++ally')
        # print(Ally)

        # print('5+++++Alltxt')
        # print(Alltxt)
        
        if noProbabilityCalc:
            plotTrendGraph(Ally,Alltxt, datelists)
        else:
            predictor,target1= trM.Transformation(dicObj=filteredTopics,window =leadTimePredict, 
                                                  GSREvents= vGsrEvents, TweetCutoff=vTweetRateCutOff,
                                                 frequency = vfrequency)
            probList = trM.Train_LogiReg(predictor,target1)
            # print(probList)
            plotTrendGraph(Ally,Alltxt, datelists,y5,yth, predictor,probList)


        # for [x * 0.1 for x in range(0, 10)]:
        # threshold = 0.1

        # step = 0.02
        # threshold = list(np.arange(0, 1 + step, step))

        if isinstance(threshold, float) or isinstance(threshold, int):
            threshold = float(threshold)
            returndic = assess_performance(datelists, probList, y5, Alltxt, threshold)
            
            print('Threshold: ' + str(threshold))
            
            print('TP: ' + str(len(returndic['TP_list'])) + ' - FN: ' + str(len(returndic['FN_list'])) +
                ' - FP: ' + str(len(returndic['FP_list'])) + ' - TN: ' + str(len(returndic['TN_list'])) +
                ' - Sum: ' + str(returndic['sum']) + ' - All: ' + str(len(returndic['all_list'])) + 
                ' - Eventcount: ' + str(len(returndic['eventcount_list'])))
            
            print('Precison: ' + str(returndic['Precison']))
            print('Recall: ' + str(returndic['Recall']))


            ### Print out row based on TP, FP, TN, FN
            ### currenlty print the whole day info want to have jsut thee keywords
            # Lst_FN = returndic['FN_list']
            
            print('-- FP --')
            print(returndic['FP_list'])
            print('-- FN --')
           
            print(returndic['FN_list'])




            '''
            print the different lists and comment tehm out
            '''
            
        elif isinstance(threshold, list):
            precision_recall_list = []
            for thresh in threshold: 
                thresh = float(thresh)
                returndic = assess_performance(datelists, probList, y5, Alltxt, thresh)

                precision_recall_dict = {}
                precision_recall_dict['Threshold'] = thresh
                precision_recall_dict['Precison'] = returndic['Precison']
                precision_recall_dict['Recall'] = returndic['Recall']
                precision_recall_list.append(precision_recall_dict)

            new_df = pd.DataFrame(precision_recall_list)
            print(new_df)

            new_df.plot(x ='Recall', y='Precison', kind = 'line')	



        else:
            print('Error. Threshold is not list or string')


    else: 
        print("Visualisation terminated check function requirements...Terminated")


def assess_performance(datelists, probList, y5, Alltxt, Threshold):
    """

    """
    TP_list = []
    FN_list = []
    FP_list = []
    TN_list = []
    all_list = []
    eventcount_list = []
    count=0
    for date in datelists:
        acc_dict = {}
        acc_dict['date']= date
        acc_dict['y5']= y5[count]
        acc_dict['probList']= probList[count]

        # acc_dict['Alltxt']= Alltxt
        all_list.append(acc_dict)

        if y5[count] == 0.5:                                     # Number of events
            eventcount_list.append(acc_dict)

        if y5[count] == 0.5 and probList[count] >= Threshold:    # TP
            TP_list.append(acc_dict)

        if y5[count] == 0.5 and probList[count] < Threshold:      # FN  
            FN_list.append(acc_dict)

        if y5[count] != 0.5 and probList[count] >= Threshold:    # FP
            FP_list.append(acc_dict)

        if y5[count] != 0.5 and probList[count] < Threshold:     # TN
            TN_list.append(acc_dict)

        count += 1
    sum = len(TP_list) + len(FN_list) + len(FP_list) + len(TN_list) 
    
    Precison = 1.0
    if (len(TP_list) + len(FP_list)) > 0:
        Precison = len(TP_list) / (len(TP_list) + len(FP_list))

    Recall = 0.0
    if (len(TP_list) + len(FN_list)) > 0:
        Recall = len(TP_list) / (len(TP_list) + len(FN_list))



    
    returndic = {}
    returndic['eventcount_list'] = eventcount_list
    returndic['TP_list'] = TP_list
    returndic['FN_list'] = FN_list
    returndic['FP_list'] = FP_list
    returndic['TN_list'] = TN_list
    returndic['all_list'] = all_list
    returndic['sum'] = sum
    returndic['Precison'] = Precison
    returndic['Recall'] = Recall


    return(returndic)

       

def prepTopicVisual(filtered,TRCutOff = 10,GSREvents=None): #I have changed the TRCutOff Default from 10 to 1
    """
    This is filters the all topics that have a support less than "TRCutOff". The function also returns the 
    "y" values for plotting the graph. These y values include the GSR event occurrence (y5), the intensity
    of the event (yth), a dictionary of the format {T_x:[date_1()...date_n()]}(allY) where T_x is the topic
    label and date_1() is the score for that topic T_x for the day date_1 and a dictionary of  summaries 
    in the format  {T_x:[date_1()...date_n()]}(alltxt) where T_x is the topic label and date_1() is the 
    summary for that topic T_x for the day date_1 
    -------------------------------------------------------------------------------
    param filtered : This is the dictionary of evidence objects after we have filtered the spurious
    topics.
    
    param TRCutOff (default =10): This is the tweetRate cutoff. We use this to filter out topics 
    with a support lower than TRCutoff.
    
    param GSREvents (default =None): This the dictionary of GSR Events.
    """
    datelists = sorted(list(filtered.keys()))
    AllTopics = list()
    for dkey in filtered.keys():
        for tkey in filtered[dkey].keys():
            AllTopics.append(tkey)
    
    AllTopics = list(set(AllTopics))
    
    allY = dict() 
    #PT. 1. This is a dictionary of List. The key is the topic and the list is the riskscore value for
    #each day for that topic
    Alltxt = dict()
    for TOI in AllTopics:
        txtSum = list()
        y9 = list()
        x = [k for k in datelists ]

        for t in datelists :
            try:
                if filtered[t][TOI].tweetrate>=TRCutOff:
                    y9.append(filtered[t][TOI].riskScore1/normConst) 
                else:y9.append(0)
            except Exception:
                y9.append(0)

        for t in datelists :
            try:
                txtSum.append(filtered[t][TOI].summary) 
            except Exception:
                txtSum.append("") 
        allY[TOI]= y9
        Alltxt[TOI]= txtSum
    
    if GSREvents:
        y5 = [GSREvents[k][0]*0.5 for k in datelists] #PT.2. Whether or not the event occurred
        yth=[GSREvents[k][1]/normConst for k in datelists] #PT.3. This is the intensity of the event
        
    else: 
        y5 = None
        yth = None
    return y5,yth,allY,Alltxt

def plotTrendGraph(Ally,Alltxt,datelists,y5 =None,yth=None,predictor=None,probList=None):
    """
    This function is the main visualisation function that plots the graph.
    ------------------------------------------------------------------------
    param Ally: a dictionary of the format {T_x:[date_1()...date_n()]}(allY) where T_x is the topic
    label and date_1() is the score for that topic T_x for the day date_1
    
    param Alltxt : a dictionary of  summaries in the format  {T_x:[date_1()...date_n()]}(alltxt) where 
    T_x is the topic label and date_1() is the  summary for that topic T_x for the day date_1 
    
    param  datalists : This is a list of dates for the period that is under consideration
    
    param y5 : This is the list of the GSR event ocurrences (Note the list here matches the datelist)
    
    param yth: This is the list of the GSR event intensity
    
    param predictor: This is the predictor values as a list. This is often the score that was derived 
    from the sentiment etc and used in the logistic regression. We like to plot this as well.
    
    param probList : This is the list of the probability values generated by the logistic regression
    trM.Train_LogiReg.
    """

    listofY = list()
    if True: # This is just to filter out the topics if we are not interested in the details
        for u in Ally.keys():
            trace_Y = Scatter(x = datelists, y = Ally[u], name = u + 'Topic_Score',
                              marker = dict(color=bupu500), text = Alltxt[u])
            listofY.append(trace_Y)
    if y5 != None: 
        #PLT.1. Here we could also use either of yth, predictor, problist etc.
        trace_gap = N=Bar(x=datelists, y=y5, name='Real World Event', marker=dict(color='#59606D'))
        
        listofY.append(trace_gap)
        if False: #Filter out overall score if we are not interested in the details
            new = predictor.tolist()
            newTarget1= [x1[0]/normConst for x1 in new]
            trace_overall = Scatter(x=datelists,y=list(newTarget1),name='Overall_Score',
                                    line=dict(color='rgb(0,0,0)',dash = 'dash') )

            listofY.append(trace_overall)

        trace_probs = Scatter(x=datelists, y=probList, name='Predicted Risk Score', mode = 'markers+lines',
                        line=dict(color='rgb(165,0,38)'),
                              marker = dict(color='rgb(165,0,38)'))
        
        listofY.append(trace_probs)

    data = listofY
    #layout = Layout(title="Risk Analysis based on Evolution of Topics", xaxis=dict(title='Date',type = "category"),
             #       yaxis=dict(title='RiskScore')
            #        , autosize =False, width =1000, height=400
            #       )
    layout = Layout(title="Topic-Risk Analysis", xaxis=dict(title='Date',type = "category"),
                    yaxis=dict(title='RiskScore')
     #               , autosize =False, width =1000, height=400
                  )

    fig = Figure(data=data, layout=layout)

    iplot(fig)



import os
from src.lib import sc_tools


def vis_from_file (Base_Path, fileout_name ):
    gvFolderLoc1  = os.path.join(Base_Path, "models", "OutputResults_batch")
    GSREvents  = sc_tools.get_pkl(os.path.join(Base_Path, 'data', 'processed', 'GSREvents.pkl'))
    dicObjfile = sc_tools.get_pkl(os.path.join(gvFolderLoc1,'GlobalVariables', fileout_name))

    bf.GSREventRead(GSREvents, dateList)