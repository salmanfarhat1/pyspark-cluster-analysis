import sys
from pyspark import SparkContext
import time
import numpy as np

def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


print("******************We started the graded lab**************************")
print("This study is for: do tasks from the same job run on the same machine")

sc = SparkContext("local[1]")
data = sc.textFile("./data/task_events/part-00000-of-00500.csv")
header = data.collect()[0].replace('"','').split(',')
lines = data.map(lambda x: x.split(","))
# print(lines.collect()[280:290])

# 2: job ID
# 3: Task ID
# 4: Machine ID
# 5: event type I took 1 because we need to see on which machine this task of this job is scheduled
pairs = lines.filter(lambda x: (x[4] != "") & (x[3] != "") & (x[2] != "") & (x[5] == "1")  ).map(lambda x : (int(x[2]) , int(x[4]) ))
# print(pairs.collect()	)
pairsGrp = pairs.distinct().groupByKey().mapValues(len);
sum = pairsGrp.reduce(lambda a,b :a+b)
print(sum)

# print("These are thje pairs that I will have for example testing ")
# print(pairsGrp.collect())

pairsGrp = pairsGrp.map(lambda x: (x[0] , True if x[1] == 1 else False));
result = pairsGrp.reduce(lambda a,b : np.logical_and(a, b))
# print(pairsGrp.collect())

print("the result is :", result)



# print ("first line in the csv machine event file is : " )
# print(header)
# print(int(''))
