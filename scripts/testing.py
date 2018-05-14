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
	'ip' 			: 'uint32',
	'click_time' 	: 'uint32',
	'is_attributed' : 'uint8'
}

columns = ['ip', 'click_time', 'is_attributed']

# ips_df = pd.read_csv("../train/train_cleaned.csv", usecols = columns, dtype = dtypes)

# print(ips_df.info())
# ips_df.head()

# size=100000
# all_rows = len(ips_df)
# num_parts = all_rows//size

# #generate the first batch
# ip_counts = ips_df[0:size][['ip', 'is_attributed']].groupby('ip', as_index=False).count()

# #add remaining batches
# for p in range(1,num_parts):
#     start = p*size
#     end = p*size + size
#     if end < all_rows:
#         group = ips_df[start:end][['ip', 'is_attributed']].groupby('ip', as_index=False).count()
#     else:
#         group = ips_df[start:][['ip', 'is_attributed']].groupby('ip', as_index=False).count()
#     ip_counts = ip_counts.merge(group, on='ip', how='outer')
#     ip_counts.columns = ['ip', 'count1','count2']
#     ip_counts['counts'] = np.nansum((ip_counts['count1'], ip_counts['count2']), axis = 0)
#     ip_counts.drop(columns=['count1', 'count2'], axis = 0, inplace=True)

# ip_counts.sort_values('counts', ascending=False)[:20]


chunkSize = 10 ** 5
chunkNumber = 0
clf = []
probs = []

train = pd.read_csv("../train/train_cleaned.csv", dtype = dtypesFull, chunksize = chunkSize)
for chunk in train:
	if chunkNumber < 2:
		print("Training chunk", chunkNumber)

		results = chunk.pop('is_attributed')
		X = chunk.as_matrix()
		Y = np.array(results)

		clf.append(RandomForestClassifier(n_estimators = 100, n_jobs = 1))
		clf[chunkNumber] = clf[chunkNumber].fit(X,Y)

		probs.append(clf[chunkNumber].predict_proba(X))
		print(clf[chunkNumber].feature_importances_)
		chunkNumber += 1
	else:
		break


chunkSize = 10 ** 6
reader = pd.read_csv('../test/test_cleaned.csv', chunksize = chunkSize)
chunkNumber = 0
with open("../predictions/predictions.csv", "w", newline = '') as writeCSV:
	writer = csv.writer(writeCSV)
	writer.writerow(["click_id","is_attributed"])
	for chunk in reader:
		print("Predicting probabilities for chunk", chunkNumber)
		
		X = chunk.as_matrix()
		aaaa = [[0 for x in range(len(clf))] for y in range(chunkSize)]
		for i in range(len(clf)):
			aaaa[:i] = clf[i].predict_proba(X)
			print(aaaa[:i])
		print(aaaa[0:100])
		for observation in aaaa:
			probs.append(np.mean(observation))
		print(len(probs))

		chunkNumber += 1


	# write to csv
	output = []
	
	for row in output:
		writer.writerow([row[0], row[1]])

	print("Done with chunk", chunkNumber, "\n")
