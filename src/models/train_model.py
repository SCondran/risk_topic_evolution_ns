#classic imports
from sklearn.linear_model import LogisticRegression
import numpy as np
import datetime

#local imports

def Transformation(dicObj,predictor=None,target=None, window =1, GSREvents =None, TweetCutoff=1, 
                   frequency ='daily'  ):
    """
    This function is just extracting the target and predictor values from the dataset (the naming
    'transformation' is perhaps misleading). In extracting the predictor variable and its values
    we simply sum the 'riskscore1' values for each topic for the day, this becomes the value for that
    day in the list of predictor values. For the target variable, we assign a 1 value to the index 
    position for that day (or hour) when the window is 1. If the window is n, we assign a 1 value to the index
    position for that day (or hour) if there was an event within n days (or hours) after the current date 
    in question.
    ---------------------------------------------------------------------------------------------------
    param dicObj : This is the dictionary of evidence objects passed in by the calling fucntion.
    
    param predictor*** (default = None): This is array that holds the predictor values. Atm moment we 
    have only one predictor variable which is the score, but we can have many- see
    sklearn.linear_model.logistic regression for more details. We dont explicitly provide the predictor
    values but rather generate it by this function. I keep this paramter here just in case we need to
    use this function by providing values.
    
    param target*** (default = None): This is array that holds the target values. We have one target variable
    which is the binary occurence of an event- see sklearn.linear_model.logistic regression for more details.
    We dont explicitly provide the target values but rather generate it by this function. I keep this paramter
    here just in case we need to use this function by providing values.
    
    param window*** (defualt =1): This is the lead time that is used to calculate the target variable.
    Eg given a window = 3 means that the target value for a current day will be set to 1 provided an event
    occured within 3 days of the current date.
    
    param GSREvents (default = None): This is the ground truth about the events that occured and which 
    dates they occured. Here we pass this in from the calling function. If it is None, then probability 
    cannot be calculated.
    
    param tweetCutoff*** (default = 1): This is passed in from the calling function. This is a cutoff below 
    which topics that have fewer than tweetCutoff tweets will be filtered out. They will not be used
    in calculating the total riskscore (from all topics) for the date.
    
    param frequency (default = 'daily'): This relates to how often the the tweets are collected. In our 
    work we consider 'daily' and 'hourly'. In this function, it is particularly important because
    it defines what a window means eg. if hourly then window =3 means 3 hours else it means 3 days. 
    
    *** - These parameters affect the results and need to be formally defined
    """
    overallScore = dict()
    if predictor==None:
        predictor = np.empty((0,1))## ide
    
    if target==None:
        target1 = np.empty((0,1))

    for eachDate in dicObj.keys():
        theScoreSum = 0.0
        for eachTopic in dicObj[eachDate].keys():
            if(dicObj[eachDate][eachTopic].tweetrate>=TweetCutoff):
                theScoreSum += dicObj[eachDate][eachTopic].riskScore1
            
        theValues = [theScoreSum,GSREvents[eachDate][0]]
        overallScore[eachDate] = theValues
        predictor = np.vstack([predictor,theScoreSum])
        
        added = False
        for i in range(window):
            if frequency == "daily":
                newkey = eachDate+datetime.timedelta(days=i)
            else: # T.1. Else it is hourly
                newkey = eachDate+datetime.timedelta(hours=i)
            
            if newkey in dicObj.keys() and GSREvents[newkey][0] == 1:
                target1 = np.vstack([target1, 1])
                added= True
                break
        if not added: target1 = np.vstack([target1, GSREvents[eachDate][0]])
            #T.2. Above sets the value in the index position for that day to original event occurrence
            #value. If there were no events withine the current date + window dates.

    return(predictor, target1)
    


def Train_LogiReg(predictor,target1):
    """
    This function trains a model and then uses that model to predict the probability of the target being
    1 for the current date (being 1 for that current date means that an event will happen within the 
    window period). In principle, it converts the 'riskscores' into a probability of an event happening.
    -------------------------------------------------------------------------------------------------
    
    param predictor: This is a single array containing the predictor values.
    
    param target1: This is a single array containing the target values.
    
    KNOWN ISSUES: The dataset needs at least an occurence each of 1 and 0 in your target values.
    """
    logitR = LogisticRegression()
    target1=target1.reshape(target1.shape[0],)
    try:
        logitR.fit(predictor,target1)
        probs= logitR.predict_proba(predictor)
        probs1 = probs.tolist()
        probList= [x[1] for x in probs1]
    except Exception:
        probList = [0] * target1.shape[0]
        print(".....Probability couldnt be calculated see function Train_LogoReg...")
    
    return probList
