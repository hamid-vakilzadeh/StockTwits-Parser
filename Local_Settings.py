# -*- coding: utf-8 -*-

# %% changes the following variables as necessary
import os
from pandas import read_excel
from pathlib import Path

DATA_FOLDER: str = "data/"

Messages_Folder = "/Volumes/Main/Databases/StockTwits/Messages/"
Activity_Folder = "/Volumes/Main/Databases/StockTwits/Activity/"
Legacy_Folder = "/Volumes/Main/Databases/StockTwits/Legacy"
ST_Folder = "/Volumes/Main/Databases/StockTwits"

LM_Sentiment = Path(DATA_FOLDER, "Inputs", "Required", "LoughranMcDonald_SentimentWordLists_2018.xlsx")
lm_dictionary = read_excel(LM_Sentiment, sheet_name = ['Negative','Positive'], header = None, engine="openpyxl")


# Create directories if not exist
Path(DATA_FOLDER, "Outputs").mkdir(parents=True, exist_ok=True)
Path(DATA_FOLDER, "Outputs", "Temp").mkdir(parents=True, exist_ok=True)