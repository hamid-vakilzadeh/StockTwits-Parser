# -*- coding: utf-8 -*-

# %% changes the following variables as necessary


Messages_Folder = "/Volumes/Main/Databases/StockTwits/Messages/"
Activity_Folder = "/Volumes/Main/Databases/StockTwits/Activity/"
Legacy_Folder = "/Volumes/Main/Databases/StockTwits/Legacy"
ST_Folder = "/Volumes/Main/Databases/StockTwits"



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