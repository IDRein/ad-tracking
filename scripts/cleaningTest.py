import numpy as np
import pandas as pd
import csv
import os
import datetime

chunkSize = 10 ** 6
cleanedFile = '../test/test_cleaned.csv'
columns = ['ip', 'app', 'device', 'os', 'channel', 'click_time']
initTime = datetime.datetime(2017, 10, 1)

test = pd.read_csv('../test/test.csv', chunksize = chunkSize)

try:
    os.remove(cleanedFile)
except OSError:
    pass

chunkNumber = 0
with open(cleanedFile, 'a+', newline = '') as cleanTestCSV:
	writer = csv.writer(cleanTestCSV)
	writer.writerow(columns)
	for chunk in test:
		chunkNumber += 1
		chunk['click_time'] = pd.to_datetime(chunk['click_time']).dt.round('H')
		chunk['click_time'] = (chunk['click_time'] -  initTime) / np.timedelta64(1, 'm')
		chunk.to_csv(cleanTestCSV, index = False, header = False)
		print("Cleaned chunk", chunkNumber)
		