#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

## Created July 13, 2015 by Mindy Foster ##

from __future__ import division
import twitter
import nltk
import datetime
import numpy as np
from sys import argv
import string
from twitter.stream import Hangup
from string import punctuation ## Uploads list of Punctuation
import arcpy
from arcpy import env
import os
import arcgisscripting
import pyper
from pyper import *
import gensim
from gensim import corpora, models, similarities, matutils
import scipy.stats as stats



def sym_kl(p,q):

	return np.sum([stats.entropy(p,q),stats.entropy(q,p)])




def arun(corpus,dictionary,l,max_topics,min_topics=1,step=1):

	kl = []
	for i in range(min_topics,max_topics,step):

		lda = models.ldamodel.LdaModel(corpus=corpus, id2word=dictionary,num_topics=i)

		m1 = lda.expElogbeta

		U,cm1,V = np.linalg.svd(m1)

		#Document-topic matrix

		lda_topics = lda[corpus]

		m2 = matutils.corpus2dense(lda_topics, lda.num_topics).transpose()

		cm2 = l.dot(m2)
		cm2 = cm2 + 0.0001

		cm2norm = np.linalg.norm(l)
		cm2 = cm2/cm2norm

		kl.append(sym_kl(cm1,cm2))

	return kl



def n_topics(dictionary, corpus):

	x = False
	while x == False:
		l = np.array([sum(cnt for _, cnt in doc) for doc in corpus])

		kl = arun(corpus,dictionary,l,max_topics=15) ## can change max_topics.. I just set to 15

		t = False
		for i in range(len(kl)):
			try:
				if kl[i] == max(kl[1:]):
					t = True
				if t == True and kl[i] < kl[i-1] and kl[i] < kl[i+1]:
					x = i+1

					break
			except Exception:
				pass
	return x

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Twitter Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [get_tweets, sentiment, topics]
	


class get_tweets(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "get_tweets"
        self.description = ""
        self.canRunInBackground = False


    def getParameterInfo(self):
        """Define parameter definitions"""
		
		#First Parameter

	param0 = arcpy.Parameter(displayName="Name of Output table", name="name", datatype="String",
			parameterType="Required", direction="Input")

	param1 = arcpy.Parameter(displayName="Keywords", name="keys", 
			datatype="GPValueTable", parameterType="Required", direction="Input")
	param1.columns = [['String', 'Query'], ['String','Group']]

        param2 = arcpy.Parameter(displayName="Collect Tweets From:", 
			name="coord", datatype="String", parameterType="Required", direction="Input")

	param2.filter.type = "ValueList"
	param2.filter.list = ['Greece', 'New York City', 'San Francisco', 'Syria', 'United States', 'Washington DC', 'Other']
	param2.value = "United States"	

	param3 = arcpy.Parameter(displayName="Other (Pair of Longitude, Latitude Coordinates SW,NW (-122.09, 32.25, -67.86, 47.16)):", 
			name="other", datatype="String", parameterType="Optional", direction="Input")
		
	param4 = arcpy.Parameter(displayName="Maximum Number of Responses", name="max", 
			datatype="String", parameterType="Required", direction="Input")
	
	

		
	params =[param0, param1, param2, param3, param4]
		
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each toole
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
	CONSUMER_KEY = '**ENTER TWITTER CONSUMER KEY HERE**'
	CONSUMER_SECRET = '**ENTER TWITTER CONSUMER SECRET HERE**'
	OAUTH_TOKEN = '**ENTER TWITTER OAUTH TOKEN HERE**'
	OAUTH_TOKEN_SECRET = '**ENTER TWITTER OAUTH SECRET HERE**''
	name = parameters[0].ValueAsText
	keys = parameters[1].ValueAsText
	keys = keys.split(';')
	keys = [key.replace("'","").split(' ') for key in keys]
	coord = parameters[2].ValueAsText
	place = ['Greece','New York City','San Francisco', 'Syria', 'United States', 'Washington DC']
	coords = ['19.98, 34.73, 26.59, 41.76','-74.26, 40.49, -73.73, 40.88','-122.75, 36.8, -121.75, 37.8','34.89, 32.52, 42.24, 37.24','-122.09, 32.25, -67.86, 47.16','-77.13, 38.81, -76.91, 39.01']
	if coord == 'Other':
		coord = parameters[3].ValueAsText
	else:
		for i in range(len(place)):
			if coord == place[i]:
				coord = coords[i]

	max = parameters[4].ValueAsText
	

	auth =  twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

	twitter_api = twitter.TwitterStream(auth=auth)

	vtab = arcpy.CreateObject("valuetable",1)
	Dir= os.getcwd()
	out_name = name +".dbf"
	arcpy.CreateTable_management(Dir, out_name)
	arcpy.AddField_management(Dir+"\\"+out_name,"user_id","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"name","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"tweet","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"lat","FLOAT")
	arcpy.AddField_management(Dir+"\\"+out_name,"long","FLOAT")
	arcpy.AddField_management(Dir+"\\"+out_name,"place","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"favorited","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"fav_count","INTEGER")
	arcpy.AddField_management(Dir+"\\"+out_name,"retweeted","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"n_friends","INTEGER")
	arcpy.AddField_management(Dir+"\\"+out_name,"query","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"group","TEXT")
	
	rows = arcpy.InsertCursor(Dir +"\\"  + out_name)

	stream = twitter_api.statuses.filter(locations=coord)
	stop_time = datetime.datetime.now()
	deltat = datetime.timedelta(seconds = 60)
	num_tweets = 0
	for tweet in stream:

		if tweet is None:
			continue
		elif 'limit' in tweet:
			continue
		elif tweet is Hangup:
			time.sleep(2.0)
			auth =  twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
			twitter_api = twitter.TwitterStream(auth=auth)
			stream = twitter_api.statuses.filter(locations=coord)
			continue
		elif tweet['text'] is None:
			continue
		else:
			for key in keys:
				if any(str(k.lower()) in tweet['text'].lower().split(' ') for k in key[:-1]):
					try:
						statement1 = tweet['text'].encode('utf-8')

						s2 = tweet['user']['id']
						s3 = tweet['user']['name']
						try:
							s4 = tweet['geo']['coordinates'][0]
							s5 = tweet['geo']['coordinates'][1]
						except Exception:
							s4 = sum(tweet['place']['bounding_box']['coordinates'],[])[0][1]
							s5 = sum(tweet['place']['bounding_box']['coordinates'],[])[0][0]

						s6 = tweet['place']['name'].encode('utf-8')
						s7 = tweet['favorited']
						s8 = tweet['favorite_count']
						s9 = tweet['retweeted']
						s10 = tweet['user']['friends_count']
						
						s11 = ' '.join(key[:-1])
						s12 = key[-1]
						s1 = statement1.splitlines()
						statement = " ".join(s1)
						row = rows.newRow()
						row.user_id = s2
						row.name = s3
						row.tweet = statement
						row.lat = s4
						row.long = s5
						row.place = s6
						row.favorited = s7
						row.fav_count = s8
						row.retweeted = s9
						row.n_friends = s10
						row.query = s11
						row.group = s12
						rows.insertRow(row)


						num_tweets += 1
				
					except Exception:
						continue
				if num_tweets >= int(max):
					break
				
				
				else:
					continue
			if num_tweets >= int(max):
				break

	return 

class sentiment(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "sentiment"
        self.description = ""
        self.canRunInBackground = False


    def getParameterInfo(self):
        """Define parameter definitions"""
		
		#First Parameter
	param0 = arcpy.Parameter(displayName="Name of Tweet Table (Don't include the file type)", 
			name="name", datatype="String", parameterType="Required", direction="Input")

	param1 = arcpy.Parameter(displayName="Group by Place", 
			name="plac", datatype="GPBoolean", parameterType="Required", direction="Input")
        
	param2 = arcpy.Parameter(displayName="Additional Grouping Parameter", 
			name="group", datatype="String", parameterType="Optional", direction="Input")
		

	params = [param0, param1, param2]
	# Copy all existing contents to output
		
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
	r = R()
	r('library(qdap)')
	name = parameters[0].ValueAsText
	Dir= os.getcwd()
	out_name = name + '.dbf'
	f = Dir +'\\'+ out_name
	rows = arcpy.SearchCursor(f)
	plac = parameters[1].ValueAsText
	try:
		group = parameters[2].ValueAsText.split(',')
	except Exception:
		pass
	if plac == 'true':
		try:
			group.append('place')
		except Exception:
			group = ['place']
	try:
		if len(group) > 0:
			out = []
			gout = []
			lates = []
			longes = []
			for row in rows:
				t = row.getValue('tweet')
				for a in list(punctuation):
					t = t.replace(a,'')
				for i in range(len(group)):
					l = row.getValue(group[i])
					l = " ".join(l.split())
					l = l.replace(' ','-')
					out.append(t.encode('ascii','ignore'))
					gout.append(l.encode('ascii','ignore'))
					a = row.getValue('lat')
					o = row.getValue('long')
					lates.append(a)
					longes.append(o)
			n = len(group)
			r.assign('x', out)
			r.assign('y', gout)
			r.assign('n',n)
			r('test <- as.data.frame(cbind(as.vector(x),matrix(y,ncol=n, byrow=TRUE)))')
			r('gro <- names(test[-1])')
			pols = r('with(test,polarity(V1, test[,gro]))')
			pols = pols.split('\n')

			out_name = name + '_' + "_".join(group) + '_polarity.dbf'
			arcpy.CreateTable_management(Dir, out_name)

			arcpy.AddField_management(Dir+"\\"+out_name,"group_var","TEXT")
			arcpy.AddField_management(Dir+"\\"+out_name,"n_sent","INTEGER")
			arcpy.AddField_management(Dir+"\\"+out_name,"n_words","INTEGER")
			arcpy.AddField_management(Dir+"\\"+out_name,"ave_pol","FLOAT")
			arcpy.AddField_management(Dir+"\\"+out_name,"sd_pol","TEXT")
			arcpy.AddField_management(Dir+"\\"+out_name,"sm_pol","TEXT")
			arcpy.AddField_management(Dir+"\\"+out_name,"lats","FLOAT")
			arcpy.AddField_management(Dir+"\\"+out_name,"longs","FLOAT")


			rows2 = arcpy.InsertCursor(Dir +"\\"  + out_name)



			for u in range(2, len(pols)-4):
				rows = arcpy.SearchCursor(f)
				row = rows2.newRow()
				po = " ".join(pols[u].split())
				po = po.split(' ')
				row.group_var = po[1]
				row.n_sent = po[-5]
       				row.n_words = po[-4]
				row.ave_pol = po[-3]
				row.sd_pol = po[-2]
				row.sm_pol = po[-1]
				for i in range(len(gout)):
					p = po[1].split('.')[-1].split('-')
					if gout[i] == ' '.join(p):
						row.lats = lates[i]
						row.longs = longes[i]
						break
				rows2.insertRow(row)
	
	except Exception:
		arcpy.AddMessage('No grouping variable detected. Calculating sentiment of each tweet.')
		arcpy.AddField_management(Dir+"\\"+out_name,"group_var","TEXT")
		arcpy.AddField_management(Dir+"\\"+out_name,"n_sent","INTEGER")
		arcpy.AddField_management(Dir+"\\"+out_name,"n_words","INTEGER")
		arcpy.AddField_management(Dir+"\\"+out_name,"ave_pol","TEXT")

		rod = arcpy.UpdateCursor(Dir+"\\"+out_name)

		for ro in rod:
			t = ro.getValue('tweet')
			r.assign("t", t)
			pols = r('polarity(t)')
			pols = pols.split('\n')
			pols = " ".join(pols[2].split())
			pols = pols.split(' ')
			ro.setValue("group_var", pols[1])
			ro.setValue("n_sent", pols[2])
       			ro.setValue("n_words", pols[3])
			ro.setValue("ave_pol", pols[4])
       			rod.updateRow(ro)

	return 

class topics(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "topics"
        self.description = ""
        self.canRunInBackground = False


    def getParameterInfo(self):
        """Define parameter definitions"""
		
		#First Parameter
	param0 = arcpy.Parameter(displayName="Name of Tweet Table (Don't include the file type)", 
			name="name", datatype="String", parameterType="Required", direction="Input")

	param1 = arcpy.Parameter(displayName="Words per Topic", 
			name="wds", datatype="String", parameterType="Required", direction="Input")
        
		

	params = [param0, param1]
	# Copy all existing contents to output
		
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
	r = R()
	r('library(qdap)')
	name = parameters[0].ValueAsText
	Dir= os.getcwd()
	out_name = name + '.dbf'
	f = Dir +'\\'+ out_name
	rows = arcpy.SearchCursor(f)
	wds = int(parameters[1].ValueAsText)
	stoplist = set(nltk.corpus.stopwords.words('english'))
	out = []
	qout = []
	gout= []
	lates = []
	longes = []
	for row in rows:
		t = row.getValue('tweet')
		q = row.getValue('query')
		g = row.getValue('group')
		for a in list(punctuation):
			t = t.replace(a, '')
		out.append(t.encode('ascii','ignore'))
		qout.append(q.encode('ascii','ignore'))
		gout.append(g.encode('ascii','ignore'))
		a = row.getValue('lat')
		o = row.getValue('long')
		lates.append(a)
		longes.append(o)

	tweets = [t.split(' ') for t in out]
	for t in range(len(tweets)):
		tweets[t] = [word for word in tweets[t] if word not in stoplist]
		tweets[t] = [word.lower() for word in tweets[t] if len(word) > 1]
	all_tokens = sum(tweets,[])

	tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
	texts = [[word for word in t if word not in tokens_once] for t in tweets]

	dictionary = corpora.Dictionary(texts)

	corpus = [dictionary.doc2bow(t) for t in texts]

	n = n_topics(dictionary, corpus)

	lda = models.ldamodel.LdaModel(corpus=corpus, id2word=dictionary, num_topics=n, iterations = 150)
	tes = lda.show_topics(num_topics=n, num_words=100, formatted=True)
	tes = [tops.replace(' + ', ',').split(',') for tops in tes]
	tes = [[str(t.split('*')[1]) for t in tops] for tops in tes]


	out_name = name + '_topics.dbf'
	arcpy.CreateTable_management(Dir, out_name)
	arcpy.AddField_management(Dir+"\\"+out_name,"tweet","TEXT")
	for b in range(n):
		arcpy.AddField_management(Dir+"\\"+out_name,"topic_"+str(b+1),"FLOAT")
	arcpy.AddField_management(Dir+"\\"+out_name,"topic","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"topic_wds","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"query","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"group","TEXT")
	arcpy.AddField_management(Dir+"\\"+out_name,"lats","FLOAT")
	arcpy.AddField_management(Dir+"\\"+out_name,"longs","FLOAT")


	rows2 = arcpy.InsertCursor(Dir +"\\"  + out_name)



	for u in range(len(out)):
		rows = arcpy.SearchCursor(f)
		row = rows2.newRow()
		row.tweet = out[u]
		a = [0]*n
		for b in range(n):
			for t in tes[b]:
				if t in out[u]:
					a[b] +=1
		for b in range(n):
			try:
				row.setValue("topic_"+str(b+1), a[b]/sum(a))
			except Exception:
				row.setValue("topic_"+str(b+1), 0)
		row.topic = "topic_" + str(a.index(max(a))+1)
       		row.topic_wds = ",".join(tes[a.index(max(a))][:wds])
		row.query = qout[u]
		row.group = gout[u]
		row.lats = lates[u]
		row.longs = longes[u]

		rows2.insertRow(row)

	return 
