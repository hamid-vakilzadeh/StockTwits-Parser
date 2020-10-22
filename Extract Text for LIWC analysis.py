#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 10:22:14 2020
@author: Hamid Vakilzadeh

This program extracts tweet text from StockTwits Messages files.

The program only extracts data from tweets that mention one ticker symbol only.

The assumption is that all the Messages files are in the same folder.
By diffuclt the files are named like "stocktwits_messages_YEAR_MONTH.gz" (e.g. stocktwits_messages_2019_03.gz).

If you have changed the default names, you need to modify the code.

When prompted You need to provide the following information:

1. The address of the directory where the Messages files are saved
2. The year for which you want to extract the data (StocktTwits data is available after 2009)

"""

# %% importing libraries
import gzip
import json
import re
import os
import pandas as pd
import datetime
import unicodedata
import time
import csv
import shutil


# %% input data 

#address = input("Please enter the address where the StockTwits Messages are located on your computer:\n")
address = "/Volumes/Main/Databases/StockTwits/Messages/"

analysis_year = input("Enter the Year for which you want to extract Tweet Information (2009 or later):\n")
#

if os.path.exists('Messages Texts') is False:
    os.mkdir('Messages Texts')
else:
    pass

destination = "Messages Texts/"

if os.path.exists("Messages Texts/Temp") is False:
    os.mkdir("Messages Texts/Temp")
else:
    pass

Temp_directory = "Messages Texts/Temp"

# %% prepare directory fil

jload = json.loads
gopen = gzip.open


files_to_analyze = [i for i in os.listdir(address) if "message" in i and analysis_year in i]
files_to_analyze.sort()

# %% Cleaning text funciton
# cleaning process to remove all, Hashtags, mentions, emojis, links...


def normalize_text(text):
    # Nomralize Text
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

def reduce_lengthening(text):
    pattern = re.compile(r"(.)\1{2,}")
    return pattern.sub(r"\1\1", text)


# %% define the tweet information extraction function
# tweet extraction process

def text_extractor(tweet):
    tweet_information = {}
    # tweet ID is unique id for each message:
    tweet_data = tweet['data']
    tweet_information['tweet_id'] = tweet_data['id']

    # tweet action whether it was created or deleted
    tweet_information['action'] = tweet['action']

    # tweet date and time
    tweet_information['action_date'] = (datetime.datetime.fromisoformat((tweet['time']).replace("Z", ""))).date()
    tweet_information['action_time'] = (datetime.datetime.fromisoformat((tweet['time']).replace("Z", ""))).time()

    # The text of the tweet
    tweet_text = tweet_data['body']
    tweet_information['text']= reduce_lengthening(normalize_text(tweet_text))

    return tweet_information

# %% Action

t = time.time()

for file in files_to_analyze:
    Temporary_file = file.replace(".gz", ".csv")
    Temporary_files = os.listdir(Temp_directory)
    if Temporary_file in Temporary_files:
        print("skipping {} becaise it was processed before.\n".format(file))
    else:
        print("Analyzing {}".format(file))
        with open(os.path.join(Temp_directory,Temporary_file),"w") as csvfile:
            field_names = ['tweet_id', 'action', 'action_date', 'action_time', 'text']
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            with gopen(os.path.join(address,file), 'rt') as f:
                for line in f:
                    tweet = jload(line)
                    if 'symbols' in tweet['data']:
                        # keep the tweets that have mentioned ONE symbol/ticker
                        if len(tweet['data']['symbols']) == 1:
                            result = [(text_extractor(tweet))]
                            writer.writerows(result)

                        else:
                            pass
                    else:
                        pass
            f.close()
        csvfile.close()

annual_data = pd.DataFrame()
Temporary_files = os.listdir(Temp_directory)
for file in Temporary_files:
    year_files = [i for i in Temporary_files if analysis_year in i]
    year_files.sort()
    annual_pd = pd.read_csv(os.path.join(Temp_directory,file))
    annual_data = annual_data.append(annual_pd)

annual_data.set_index(['tweet_id'], inplace=True)

destination_file_name = os.path.join(destination, "{}.csv".format(analysis_year))
annual_data.to_csv(destination_file_name)

shutil.rmtree(Temp_directory)
t2 = time.time()
duration = t2-t
mon, sec = divmod(duration, 60)
hr, mon = divmod(mon, 60)
print("{} was analyzed in ".format(analysis_year)+"%d:%02d:%02d" % (hr, mon, sec))
print('{:,} tweets were extracted from {}.'.format(len(annual_data), analysis_year))

