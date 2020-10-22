#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This program extracts Activity data from StockTwits Acitivity Files
The underlying database is propritery and you need to request a license from StockTwits.

This code assumes that all the Activity files are in the same folder.
By diffuclt the files are named like "stocktwits_activity_YEAR_MONTH.gz" (e.g. stocktwits_activity_2019_03.gz).

@author: 
    Hamid Vakilzadeh
    University of Wisconsin Whitewater

"""
# %% required packages


import time
import pandas as pd
import json
import gzip
import os

# %% Extract Like Data


t = time.time()
jload = json.loads
gopen = gzip.open

source = input("This Code Extracts StockTwits Acitivity Data, To get started please enter the folder that contains the Acitivity zip files:\n")


year = input("Enter the Year for which you want to extract Activity Information (2009 or later):\n")

destination = input("Enter the complete address where you want to save the files:\n")


# %% Function to convert activity json files to DataFrames and extract them as CSV


def extract_activity(source, destination):
    files = os.listdir(source)
    destination_folder = os.listdir(destination)

    activity_files = [file for file in files if "stocktwits_activity" in file and year in file]
    activity_files.sort()
    for file in activity_files:
        destination_file_name = os.path.join(destination, file).replace(".gz", ".csv")
        destination_file = file.replace(".gz", ".csv")
        if destination_file in destination_folder:
            print("{} is already processed.\n".format(file))
            pass
        else:
            print("Analyzing {}...".format(file))
            with gopen(os.path.join(source, file), 'rt') as f:
                Activity = [jload(line) for line in f]
            f.close()

            Activity_pd = pd.json_normalize(Activity)
            Activity_pd['time'] = pd.to_datetime((Activity_pd['time']), format="%Y%m%dT%H:%M:%S", utc=True)
            Activity_pd['Date'] = pd.to_datetime(Activity_pd['time']).dt.date
            Activity_pd.to_csv(destination_file_name)

            print("{} Analyzed.\n\n".format(file))


extract_activity(source, destination)
