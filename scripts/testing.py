import numpy as np
import pandas as pd
import csv
from dateutil import parser
import datetime
from datetime import timedelta
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
import operator
import sys
import dask
import dask.dataframe as dd



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

# with more training data,
# we will chunk the csv and do this section by section for memory
train = pd.read_csv("../train_sample/train_sample.csv", usecols = columns, dtype = dtypes)
train['click_time'] = pd.to_datetime(train['click_time']).dt.round('H')
train['click_time'] = (train['click_time'] - train['click_time'].min()) / np.timedelta64(1, 'h')
print("Done Parsing\n")
results = train.pop('is_attributed')
X = train.as_matrix()
Y = np.array(results)

print("Begun Training...")
clf = RandomForestClassifier(n_estimators = 300)
clf = clf.fit(X,Y)
print("Done Training\n")

sys.exit()

# probs = clf.predict_proba(X)
# print(probs)
# print(Y)
# i = 0
# for entry in Y:
# 	if (entry == 1):
# 		print(probs[i])
# 	i += 1
# print("Cross Validating...\n")
# scores = cross_val_score(clf, X, Y)
# print(scores.mean())




chunkSize = 500000
reader = pd.read_csv('../test.csv/test.csv', chunksize = chunkSize)
print("Begin parsing test file...")
status = 0
chunkNumber = 0
with open("../predictions/predictions.csv", "w", newline = '') as writeCSV:
	writer = csv.writer(writeCSV)
	writer.writerow(["click_id","is_attributed"])

	# create initTime array
	for i in range(0, chunkSize):
		initTime.append(t)
	for chunk in reader:
		chunkNumber += 1
		print(chunk.iloc[0].tolist())

		dt = chunk.iloc[:,6].tolist()
		for i in range(0, len(dt)):
			dt[i] = parser.parse(dt[i])
		dt = list(map(operator.sub, dt, initTime))
		dt = list(map(timedelta.total_seconds, dt))
		chunk['click_time'] = dt
		ids = chunk.pop('click_id').tolist()
		X = chunk.as_matrix()
		probs = clf.predict_proba(X)

		# write to csv
		output = []
		for i in range(0, len(dt)):
			line = [int(ids[i]), probs[i][1]]
			if (line[1] > 0.5):
				print(line)
			output.append(line)
		for row in output:
			writer.writerow([row[0], row[1]])

		print("Done with chunk", chunkNumber, "\n")
