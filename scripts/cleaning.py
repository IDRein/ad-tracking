import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import csv
import os
import datetime

# preset datatypes
dtypes = {
        'ip'            : 'uint32',
        'app'           : 'uint16',
        'device'        : 'uint16',
        'os'            : 'uint16',
        'channel'       : 'uint16',
        'is_attributed' : 'uint8',
        }

columns = ['ip', 'app', 'device', 'os', 'channel', 'click_time', 'is_attributed']

chunkSize = 10 ** 3

initTime = datetime.datetime(2017, 10, 1)

train = pd.read_csv("../train/train_sample.csv", 
	chunksize = chunkSize, 
	dtype = dtypes, 
	usecols = columns)

cleanedFile = '../train/train_cleaned.csv'

try:
    os.remove(cleanedFile)
except OSError:
    pass

with open(cleanedFile, 'a+', newline = '') as cleanedCSV:
	writer = csv.writer(cleanedCSV)
	writer.writerow(columns)
	for chunk in train:
		chunk['click_time'] = pd.to_datetime(chunk['click_time']).dt.round('H')
		chunk['click_time'] = (chunk['click_time'] -  initTime) / np.timedelta64(1, 'm')
		chunk.to_csv(cleanedCSV, index = False, header = False)

