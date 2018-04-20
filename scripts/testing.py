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
dtypesFull = {
	'ip'            : 'uint32',
	'app'           : 'uint16',
	'device'        : 'uint16',
	'os'            : 'uint16',
	'channel'       : 'uint16',
	'click_time'	: 'uint32',
	'is_attributed' : 'uint8'
}

dtypes = {
	'ip' : 'uint32',
	'click_time' : 'uint32',
	'is_attributed' : 'uint8'
}

columns = ['ip', 'click_time', 'is_attributed']

ips_df = pd.read_csv("../train/train_cleaned.csv", usecols = columns, dtype = dtypes)

print(ips_df.info())
ips_df.head()

size=100000
all_rows = len(ips_df)
num_parts = all_rows//size

#generate the first batch
ip_counts = ips_df[0:size][['ip', 'is_attributed']].groupby('ip', as_index=False).count()

#add remaining batches
for p in range(1,num_parts):
    start = p*size
    end = p*size + size
    if end < all_rows:
        group = ips_df[start:end][['ip', 'is_attributed']].groupby('ip', as_index=False).count()
    else:
        group = ips_df[start:][['ip', 'is_attributed']].groupby('ip', as_index=False).count()
    ip_counts = ip_counts.merge(group, on='ip', how='outer')
    ip_counts.columns = ['ip', 'count1','count2']
    ip_counts['counts'] = np.nansum((ip_counts['count1'], ip_counts['count2']), axis = 0)
    ip_counts.drop(columns=['count1', 'count2'], axis = 0, inplace=True)

ip_counts.sort_values('counts', ascending=False)[:20]

sys.exit()

# with more training data,
# we will chunk the csv and do this section by section for memory
train = pd.read_csv("../train/train_sample.csv", dtype = dtypesFull)
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
