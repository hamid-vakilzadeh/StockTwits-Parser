#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 20:37:25 2020

This code extracts Tweet Information from StockTwits Archival Data using Messages files
The file extracts information for each requested year
You need to provide 
1. The address of the directory where the Messages files are saved
2. The year for which you want to extract the data (between 2009 and 2020)



@author: Hamid Vakilzadeh
"""

# %% importing libraries
import gzip
import json
import re
import os
import pandas as pd
import datetime
import unicodedata
from vaderSentiment import vaderSentiment
from textblob import TextBlob
import csv
import shutil
import time
import numpy as np
from Local_Settings import Activity_Folder, Legacy_Folder, Messages_Folder
from argparse import ArgumentParser
# import html

# %% input data 


# address = input("Please enter the address where the StockTwits Messages are located on your computer:\n")
address = "/Volumes/Main/Databases/StockTwits/Messages/"

analysis_year = input("Enter the Year for which you want to extract Tweet Information:\n")
#

if os.path.exists('Messages') is False:
    os.mkdir('Messages')
else:
    pass

destination = "Messages/"

if os.path.exists("Messages/Temp") is False:
    os.mkdir("Messages/Temp")
else:
    pass

Temp_directory = "Messages/Temp"

# %% prepare directory fil

jload = json.loads
gopen = gzip.open

files_to_analyze = [i for i in os.listdir(address) if "message" in i and analysis_year in i]
files_to_analyze.sort()
lm_dictionary = pd.read_excel("LoughranMcDonald_SentimentWordLists_2018.xlsx", sheet_name = ['Negative','Positive'], header = None)



# %% open Loughran and McDonald's sentiment dictionary

lm_negative = lm_dictionary.get('Negative')[0].tolist()
lm_positive = lm_dictionary.get('Positive')[0].tolist()

def loughran_scores(text):
    twords = text.split()
    twords2 = [i.upper() for i in twords]
    len_twords2 = len(twords2)
    negative_found = len([i for i in twords2 if i in lm_negative])
    positive_found = len([i for i in twords2 if i in lm_positive])
    return(negative_found, positive_found, len_twords2)


# %% investigate the content of Twits and Acitivites Json Files
# Color code the Json file for investigation and viewing the order of a tweet
'''
Twits_json_example = (json.dumps(Twits[1], indent=4, sort_keys=True))

colorful__Twits_json = highlight(Twits_json_example, lexers.JsonLexer(), formatters.TerminalFormatter())
print(colorful__Twits_json)
'''

# %% Cleaning text funciton
# cleaning process to remove all, Hashtags, mentions, emojis, links...


def normalize_text(text):
    """Nomralize Text
    """
    text = unicodedata.normalize("NFKD", text)  # Normalize
    text = '\n'.join(
        text.splitlines())  # Let python take care of unicode break lines

    # Convert to upper
    # text = text.upper()  # Convert to upper

    # Clean breaklines & whitespaces combinations due to beautifulsoup parse
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # To find MDA section, reformat item headers
    text = text.replace('\n.\n', '.\n')  # Move Period to beginning

    text = text.replace('\nI\nTEM', '\nITEM')
    text = text.replace('\nITEM\n', '\nITEM ')
    text = text.replace('\nITEM  ', '\nITEM ')

    text = text.replace(':\n', '.\n')

    # Math symbols for clearer looks
    text = text.replace('$\n', '$')
    text = text.replace('\n%', '%')

    # Reformat
    text = text.replace('\n', '\n\n')  # Reformat by additional breakline

    # Remove emojis
    RE_EMOJI = re.compile(pattern="["
                          u"\U0001F600-\U0001F64F"  # emoticons
                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                          u"\U0001F680-\U0001F6FF"  # transport & map symbols
                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                          u"\U00002702-\U000027B0"
                          u"\U000024C2-\U0001F251"
                          u"\U00010000-\U0010ffff"
                          "]+", flags=re.UNICODE)
    text = RE_EMOJI.sub(r'', text)

    # Remove URLs
    text = re.sub(r"http\S+", '', text)

    # Remove Hashtags
    text = re.sub(r"(?<!&)#\S+", '', text)

    # Remove @sign
    text = re.sub(r"(@\S+)", '', text)

    # Remove $sign
    text = re.sub(r"\$\S+", '', text)

    return text


# %% Define VaderSentiment function
# define Vader Sentiment analysis package


vs = vaderSentiment.SentimentIntensityAnalyzer()

def vsp(text):
    t = vs.polarity_scores(text)
    return t

def reduce_lengthening(text):
    pattern = re.compile(r"(.)\1{2,}")
    return pattern.sub(r"\1\1", text)


# %% define the tweet information extraction function
# tweet extraction process

def tweet_extractor(tweet):
    tweet_information = {}
    # tweet ID is unique id for each message:
    tweet_data = tweet['data']
    tweet_information['tweet_id'] = tweet_data['id']

    # tweet action whether it was created or deleted
    tweet_information['action'] = tweet['action']

    # tweet date and time
    tweet_information['date_created'] = (datetime.datetime.fromisoformat((tweet_data['created_at']).replace("Z", ""))).date()
    tweet_information['time_created'] = (datetime.datetime.fromisoformat((tweet_data['created_at']).replace("Z", ""))).time()

    tweet_information['action_date'] = (datetime.datetime.fromisoformat((tweet['time']).replace("Z", ""))).date()
    tweet_information['action_time'] = (datetime.datetime.fromisoformat((tweet['time']).replace("Z", ""))).time()

    # Dummy variable for official accounts (official =1):
    user_data = tweet['data']['user']
    if user_data['official'] is True:
        tweet_information['official_user'] = 1
    else:
        tweet_information['official_user'] = 0

    # Dummy variable for users suggested by StockTwits
    if 'suggested' in user_data['classification']:
        tweet_information['suggested_user'] = 1
    else:
        tweet_information['suggested_user'] = 0

    # number of followers
    tweet_information['follwers'] = user_data['followers']
    
    # number of subscribers
    tweet_information['subscribers'] = user_data['subscribers_count']

    # Tweet is a part of conversation or not
    if 'conversation' in tweet_data:
        tweet_information['conversation'] = 1
        # conversation length
        tweet_information['conversation_replies'] = tweet_data['conversation']['replies']
        # Main Tweet in a conversation
        if tweet_data['conversation']['parent'] is True:
            tweet_information['conversation_starter'] = 1
        else:
            tweet_information['conversation_starter'] = 0
    else:
        tweet_information['conversation'] = 0
        tweet_information['conversation_starter'] = None
        tweet_information['conversation_replies'] = 0


    # the ticker symbol mentioned in the tweet
    tweet_information['ticker'] = tweet_data['symbols'][0]['symbol']

    # entities in the tweet (chart, sentiment, video, link)
    entities = [i for i in tweet_data['entities']]
    tweet_information['entities'] = len(entities)

    # extract user-posted sentiment if there is any:
    if tweet_data['entities']['sentiment'] is not None:
        tweet_information['sentiment'] = tweet_data['entities']['sentiment']['basic']
    else:
        tweet_information['sentiment'] = None

    # if the text has a sentiment score we record it:
    if 'sentiment' in tweet_data:
        tweet_information['website_sentiment_score'] = tweet_data['sentiment']['sentiment_score']
    else:
        tweet_information['website_sentiment_score'] = None

    # tweet charts
    tweet_information['chart'] = 1 if 'chart' in tweet_data['entities'] else 0

    # links mentioned in the tweet
    if 'links' in tweet_data:
        tweet_information['mentioned_links'] = 1
        tweet_information['number_of_links'] = len(tweet_data['links'])
        tweet_information['mentioned_link_address'] = tweet_data['links'][0]['url']
        tweet_information['link_domain_name'] = tweet_data['links'][0]['source']['name']
    else:
        tweet_information['mentioned_links'] = 0
        tweet_information['number_of_links'] = 0
        tweet_information['mentioned_link_address'] = None
        tweet_information['link_domain_name'] = None
    # The text of the tweet
    tweet_text = tweet_data['body']
    #tweet_information['text']=tweet_text
    '''
    calculate Sentiment scores based on the text of the tweet

    I calculate the scores using multiple popular measures:
    The following calcuation is based on VADER Sentiment following:

        "Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment
        Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media
        (ICWSM-14). Ann Arbor, MI, June 2014."

    This measure is very suitable for social media content.
    '''
    # These VADER scores are generated based on the original text of the tweet:
    reduced_length_tweet_text = reduce_lengthening(tweet_text)
    tb = TextBlob(reduced_length_tweet_text)
    tweet_text_correct = str(tb)
    vader_scores = vsp(tweet_text_correct)
    tweet_information['vader_neg'] = (vader_scores)['neg']
    tweet_information['vader_neu'] = (vader_scores)['neu']
    tweet_information['vader_pos'] = (vader_scores)['pos']
    tweet_information['vader_compund'] = (vader_scores)['compound']

    # These VADER scores are generated based on the cleaned text of the tweet (normalized)
    clean_tweet_text = normalize_text(tweet_text)
    vader_scores_clean = vsp(clean_tweet_text)
    tweet_information['vader_neg_cleaned'] = (vader_scores_clean)['neg']
    tweet_information['vader_neu_cleaned'] = (vader_scores_clean)['neu']
    tweet_information['vader_pos_cleaned'] = (vader_scores_clean)['pos']
    tweet_information['vader_compund_cleaned'] = (vader_scores_clean)['compound']

    '''
    The second measure is Text Blob which measures the polarity and subjectivity
    polarity is between -1 and +1
    subjectivity is between 0 and 1 where zero is highly objective.
    '''
    # Text Blob scores
    tweet_information['textblob_polarity'] = tb.sentiment.polarity
    tweet_information['textblob_subjectivity'] = tb.sentiment.subjectivity

    # Loughran sentiment values
    my_loughran_scores = loughran_scores(clean_tweet_text)
    tweet_information['loughran_negative'] = my_loughran_scores[0]
    tweet_information['loughran_positive'] = my_loughran_scores[1]
    tweet_information['loughran_length'] = my_loughran_scores[2]
    return tweet_information

# %% Action

destination_folder = os.listdir(destination)
destination_file_name = "{}_messages.csv".format(analysis_year)

try:
    if destination_file_name in destination_folder:
        raise("{} already exists. if you want to recreate this file delete the file from \'{}\' directory and try again.".format(destination_file_name,Temp_directory))
        shutil.rmtree(Temp_directory)
    else:
        t = time.time()
        for file in files_to_analyze:
            try:
                Temporary_file = file.replace(".gz", ".csv")
                Temporary_files = os.listdir(Temp_directory)
                if Temporary_file in Temporary_files:
                    print("skipping {} because it was processed before.\n".format(file))
                else:
                    print("Analyzing {}".format(file))
                    with open(os.path.join(Temp_directory,Temporary_file),"w") as csvfile:
                        field_names = ['tweet_id', 'action', 'date_created', 'time_created', 'action_date', 'action_time', 'official_user', 'suggested_user', 'follwers', 'subscribers', 'conversation', 'conversation_starter', 'conversation_replies', 'ticker', 'entities', 'sentiment', 'website_sentiment_score', 'chart', 'mentioned_links', 'number_of_links', 'mentioned_link_address', 'link_domain_name', 'vader_neg', 'vader_neu', 'vader_pos', 'vader_compund', 'vader_neg_cleaned', 'vader_neu_cleaned', 'vader_pos_cleaned', 'vader_compund_cleaned', 'textblob_polarity', 'textblob_subjectivity', 'loughran_negative', 'loughran_positive', 'loughran_length']
                        writer = csv.DictWriter(csvfile, fieldnames=field_names)
                        writer.writeheader()
                        with gopen(os.path.join(address,file), 'rt') as f:
                            for line in f:
                                tweet = jload(line)
                                if 'symbols' in tweet['data']:
                                    # keep the tweets that have mentioned ONE symbol/ticker
                                    if len(tweet['data']['symbols']) == 1:
                                        result = [(tweet_extractor(tweet))]
                                        writer.writerows(result)
                                    else:
                                        pass
                                else:
                                    pass
                        f.close()
                    csvfile.close()
            except:
                os.remove(os.path.join(Temp_directory,Temporary_file))
                break

        annual_data = pd.DataFrame()
        Temporary_files = os.listdir(Temp_directory)
        year_files = [i for i in Temporary_files if analysis_year in i]
        year_files.sort()
        for file in year_files:
            annual_pd = pd.read_csv(os.path.join(Temp_directory,file))
            print("reading {} columns from {}".format(len(annual_pd.columns),file))
            annual_data = annual_data.append(annual_pd)

        annual_data.set_index(['tweet_id'], inplace=True)

        destination_file_name = os.path.join(destination, destination_file_name)
        annual_data.to_csv(destination_file_name)

        shutil.rmtree(Temp_directory)
        t2 = time.time()
        duration = t2-t
        mon, sec = divmod(duration, 60)
        hr, mon = divmod(mon, 60)
        print("{} was analyzed in ".format(analysis_year)+"%d:%02d:%02d" % (hr, mon, sec))
        print('{:,} tweets were extracted from {}.'.format(len(annual_data), analysis_year))

except:
    print("process interrupted")

with gopen(os.path.join(address, files_to_analyze[0]), 'rt') as f:
    Activity = [jload(line) for line in f]
f.close()
message_pd = pd.json_normalize(Activity,max_level=5)
message_pd['data.symbols'].replace(np.nan, '', regex=True, inplace=True)
message_pd[["id", "symbol", "title", "exchange", "sector", "industry"]] = pd.DataFrame(message_pd["data.symbols"].tolist(), index=message_pd.index)