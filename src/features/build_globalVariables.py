import os

#This is where we define all the global variables

#Local Imports
from src.features import build_features as bf
## For defined functions

def initGV():
    """
    This initialiseses the global variables namely globalTopics.pkl, theTopicNo.pkl and
    topicFeatureMap.pkl described as follows: 
        globalTopics -  is of the format {date: {T_x:[keywords,TweetIds]}} where
        date is the key containd topic labels. Each topic label is a key containing a list. The first 
        value of the list is a list of keywords describing the topic and the second is a list of the
        tweetIds that are associated with th topic.
        theTopicNo - this holds the last index value 'x' in T_x which makes the labels of each new 
        topic unique.
        topicFeatureMap - this is a dictionary of the format {keyword:index} where keyword is the feature
        and index is the assigne index position for the columns features. The reason we keep this is that
        it stores all the words that are ever used in describing a topic, this means we can use this vector
        space to determine the distance between topics. We update this by adding new topic words that didnt
        exist in it previously (words that now appear in the topics, but didnt in previous analysis)
    """
    global globalTopics
    globalTopics= dict()
    
    global theTopicNo
    theTopicNo = 0
    
    global topicFeatureMap
    topicFeatureMap = dict()
    
    #global normConst
    #normConst = 25
    
    #global allTopics
    #allTopics = list()
    
def updateTopicFeatureMap (wordList):
    """
    This adds words from the newly discovered topics that didnt previously exist in the topicfeaturemap
    -------------------------------------------------------------------------------------------------
    param wordList : This a list of words describing a topic.
    """
    global topicFeatureMap
    if topicFeatureMap:
        tempIndicesList = sorted(list(topicFeatureMap.values()))
        curIndex = tempIndicesList[-1] + 1
    else:
        curIndex=0
    for everyWord in wordList:
        if everyWord not in topicFeatureMap.keys():
            topicFeatureMap[everyWord] = curIndex
            curIndex +=1
            
def updateGV(gvFolderLoc):
    """
    This function reads in the stored global variables and uses these in the processing rather than
    creating new values.
    ---------------------------------------------------------------------------------------------
    param gvFolderLoc: This is the folder that contains the global variables as files namely,
    globalTopics.pkl, theTopicNo.pkl, topicFeatureMap.pkl and som.pkl(not required here).
    """
    global globalTopics
    global topicFeatureMap
    global theTopicNo
    globalTopics = bf.getData_2(os.path.join(gvFolderLoc, "globalTopics.pkl"))    
    theTopicNo = bf.getData_2(os.path.join(gvFolderLoc, "theTopicNo.pkl"))
    topicFeatureMap = bf.getData_2(os.path.join(gvFolderLoc, "topicFeatureMap.pkl"))
    
    #global normConst
    #normConst = 25
  
    
    
    