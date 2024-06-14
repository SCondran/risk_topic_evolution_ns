#here we will create a separate class later in a different file and use the import statements to 
#import the class. Remember that this class itself migh require some imports. But this schould be be 
#fine if we import the required modules via the main file.
#Classical Classes
import math
import operator

#Local imports
import src.features.build_globalVariables as gv  ## For global variables





class Evidence:
    """
    This is class creates objects that represent the daily topical summaries and risk scores.
    Each object describes a topic and has the following properties:
    -----------------------------------------------------------------------------------
    property date : this is the date of the topic
    
    property sentiment***: This is the sentiment calculated for that topic - At the moment we
    are considering the summative (average) negative sentiments of the tweets related to that topic.
    
    property polRatio***: This is the ratio of the number of negative sentiments to the number of 
    total tweets
    
    property gradient***: This is the rate of change of the 'exactRiskScore' for a topic between dates.
    
    property exactRiskScore***: This is the harmonic mean between the sentiment value and the polarityRatio 
    in the first instance. when the gradient is calculated from the calculated exactRiskScores, then we 
    recalculate a new exactRiskScore which is the harmonic mean  ofe sentiment value, polarityRatio
    and gradient.
    
    property riskScore***: This is the moving average of the exactRiskScore for a specified window -
    given by the updateObj function.
    
    property riskScore1***: This is the final Score to be used and is calculated as the geometric mean between
    the riskScore and the tweetRate - the value can be greater than 1, but we often use a moderation
    constant to limit the value to between 0 and 1 (usually). This moderation is not necessary because 
    in the end we only use the values of riskscor1 as a predictor for a logistic regression, whose results are
    unaffected by the actual nominal values used. We moderate it however for visualisation purpose ,
    so that the graph is not unecessarily 'squeezed'.
    
    property tweetRate***: This is the 'total' number of tweets for that topic. 
    
    property summary: This stores the relevant summaries of the object as a string so that we can 
    retrieve it when necessary. It contains the sentiment score, polarityratio, tweetrate and the 
    keywords for that topic.
    
    *** - These properties affect the results of the analysis and recquire some strong justifications
    and formal definitions.
    """
    def __init__(self, date, sentiment, polRatio, tweetrate):
        self.date = date
        self.sentiment = sentiment
        self.polRatio = polRatio
        self.gradient = 0.0
        self.riskScore = 0.0
        self.exactRiskScore =0.0
        self.tweetrate = tweetrate
        self.riskScore1 = 0.0
        self.summary = None
    
    def updateObj(self,topic, dicObj,dateList, window=1):
        """
        This calculates the score (riskcore1) for every date and every topic within that date.
        -------------------------------------------------------------------------------------------
        param topic: this is the topic to be updated
        
        param dicObj: this is the dictionary containing {Date:{topic:[date,sentiment,etc]}}, we need this
        because when we calculate the gradient so we need the previous history.
        
        param window***: This is the window period for which we calculate the moving average. Typically we use
        a default of 1.
        
        *** - These parameters affect the results of the work and need to be formally defined and supported.
        """
        keyList = sorted(dateList)
        keyIndex = keyList.index(self.date)
        f = self.sentiment + self.polRatio
        if f <=0: f =1
        if self.date == keyList[0]:
            self.gradient = 1
            self.exactRiskScore = self.sentiment*self.polRatio*1
        
        else:
            prevKey = keyList[keyIndex-1]
            self.exactRiskScore = 2*(self.sentiment * self.polRatio)/f
            try:
                prevRiskWoGrad = 2(dicObj[prevKey][topic].sentiment*dicObj[prevKey][topic].polRatio)\
                /dicObj[prevKey][topic].sentiment+dicObj[prevKey][topic].polRatio
            except Exception:
                prevRiskWoGrad =0
            
            self.gradient = ((self.exactRiskScore-prevRiskWoGrad) + 1)/2
            
            f1= self.sentiment * self.polRatio
            f2=self.polRatio*self.gradient
            f3=self.sentiment*self.gradient
            sumf = f1+f2+f3
            if sumf <= 0:
                sumf = 1
            self.exactRiskScore = 3*(self.sentiment * self.polRatio*self.gradient)/(sumf)
            
            
        try:
            if (keyIndex+1 -window)>=0:
                self.riskScore = sum([dicObj[keyList[t]][topic].exactRiskScore for t in 
                                      range(keyIndex+1-window,keyIndex+1)])/window
            else:
                self.riskScore = sum([dicObj[keyList[t]][topic].exactRiskScore for t in 
                                      range(0,keyIndex+1)])/(keyIndex+1)
            
        except Exception:
            self.riskScore = self.exactRiskScore
            
        
        self.riskScore1 = math.sqrt(self.riskScore*self.tweetrate)    
            
        
    
    def updateSummary(self,topic, n_top_words=100):
        """
        This updates the summaries for each topic. The summaries include the sentiment, the polarity ratio,
        the tweetrate and the keywords.
        -----------------------------------------
        param topic: This is the topicfor which we want to create the summary.
        
        param n_top_words (default =100): This is the maximum number of top words in the topic we want 
        to store.
        """
        n_top_words_dic = gv.globalTopics[self.date][topic] 
        self.summary = "S: " + str(round(self.sentiment, 2)) + '\n' + \
                        "PR: "+ str(round(self.polRatio,2)) + '\n' + \
                        "TR: "+ str(round(self.tweetrate,2)) + '\n' + \
                        "keyWords: "+  '\n' + \
                        str(n_top_words_dic[0][0:n_top_words])+  '\n'
                        
     #***********************The following Functions are not in use anymore**********************************   
    def getSummary_original(self,indo_EngTweetsPd_sent, n_top_words):
        """
        Not in use atm
        """
        pubYear =int(self.date[0:4])
        pubMonth =int(self.date[5:7])
        pubDay = int(self.date[8:10])
        global tf
        tfArr = tf.getrow(dateMap[self.date][0]).toarray()
        tfList = tfArr.tolist()[0]
        dicWords =dict()
        for i in range(len(tfList)):
            #print (i)
            dicWords[i] = tfList[i]
        sorted_x = sorted(dicWords.items(), key=operator.itemgetter(1), reverse =True)
        n_top_words_dic ={}
        y=0
        while  y < n_top_words:
            n_top_words_dic[tf_vectorizer.get_feature_names()[sorted_x[y][0]]] = sorted_x[y][1] 
            y= y+1
        
        
        
        global tf1
        tf1Arr = tf1.getrow(dateMap[self.date][0]).toarray()
        tf1List = tf1Arr.tolist()[0]
        dicWords =dict()
        for i in range(len(tf1List)):
            #print (i)
            dicWords[i] = tf1List[i]
        sorted_x = sorted(dicWords.items(), key =operator.itemgetter(1), reverse =True)
        n_top_words_dic1 ={}
        y=0
        while  y < n_top_words:
            n_top_words_dic1[tf1_vectorizer.get_feature_names()[sorted_x[y][0]]] = sorted_x[y][1] 
            y= y+1
        #print (n_top_words_dic)
        
        
        
        indo_EngTweetsPdCur= indo_EngTweetsPd_sent[(indo_EngTweetsPd_sent['publicationYear']==pubYear) & \
                                                   (indo_EngTweetsPd_sent['publicationMonth']==pubMonth) & \
                                                   (indo_EngTweetsPd_sent['publicationDay']==pubDay)]
        tempDatafr = indo_EngTweetsPdCur.copy(deep=True)
        top_ten_worst=[]
        y=0
        while (y < 10) and (not tempDatafr.empty):
           # print(tempDatafr['sentiment'])
           # print(tempDatafr['sentiment'].idxmin())
            try:
                worstTweet = tempDatafr.loc[tempDatafr['sentiment'].idxmin()]
            except Exception:
                break
                #worstTweet= "DONE"# should be an object
            #worstTweet = tempDatafr.loc[tempDatafr['sentiment'].idxmin()]
            
            #if not top_ten_worst:
                #top_ten_worst.append((worstTweet.text,worstTweet.sentiment) )
            #print("dear", worstTweet.text)
            doit= True
            for tweets in top_ten_worst:
                if similar(cleanText_udf_new(worstTweet.text), tweets)>0.8:
                    doit =False
                    break
            if(doit):
                #print("deary", worstTweet.text)
                top_ten_worst.append((cleanText_udf_new(worstTweet.text) + "-" + str(worstTweet.sentiment)) )
                y= y+1
            tempDatafr.drop(tempDatafr['sentiment'].idxmin(), inplace =True)
            
        print("The evidence for this day is as follows: ")
        print(" DATE: ", self.date,\
              "\n SentimentScore: ", self.sentiment,\
              "\n Polarity Ratio: ", self.polRatio,\
              "\n Gradient: ", self.gradient,\
              "\n Risk Score1: ", self.riskScore1,\
              "\n Exact Risk Score: ", self.exactRiskScore,\
              "\n tweetRate: ", self.tweetrate,\
             #"\n TOP TEN Words: ", n_top_words_dic,\
             
              #"\n Top Ten Worst Tweets: ", top_ten_worst  
             )
        print("\n TOP TEN Words: ")
        for key in n_top_words_dic.keys():
            
            print ( key, n_top_words_dic[key])
            print()
        print("\n TOP TEN Words: ")
        for key in n_top_words_dic1.keys():
            
            print ( key, n_top_words_dic1[key])
            print()
        print("\n Top Ten Worst Tweets: ")
        for each in top_ten_worst:
            
            print (each)
            print()
        #print("Date: ", self.date)
        
        
    #def getKeywords(self,indo_EngTweetsPd_sent):
    
    def getWC(self,indo_EngTweetsPd_sent):
        """
        No longer used atm
        """
        pubYear =int(self.date[0:4])
        pubMonth =int(self.date[5:7])
        pubDay = int(self.date[8:10])
        indo_EngTweetsPdCur= indo_EngTweetsPd_sent[(indo_EngTweetsPd_sent['publicationYear']==pubYear) & \
                                                   (indo_EngTweetsPd_sent['publicationMonth']==pubMonth) & \
                                                   (indo_EngTweetsPd_sent['publicationDay']==pubDay)]
        
        dayTweetList =indo_EngTweetsPdCur['text'].tolist()
        newDayTweetList= ", ".join(dayTweetList)
        cleanText = cleanText_udf(newDayTweetList)
        WCgen(cleanText)
        
    
    
            