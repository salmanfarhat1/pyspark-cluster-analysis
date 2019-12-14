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
print("This study is for: What is the distribution of the number jobs/tasks per scheduling class")

sc = SparkContext("local[1]")
data = sc.textFile("./data/task_events/part-00000-of-00500.csv")

lines = data.map(lambda x: x.split(","))



#removing all the duplicates
pairsJob = lines.filter(lambda x: (x[7] !="") ).map(lambda x: (int(x[2]) , int(x[7]))).distinct()
pairsTask = lines.filter(lambda x: (x[7] !="") ).map(lambda x: (int(x[3]) , int(x[7]))).distinct()

#now groupby the value and calculate number of tids / jids that we have per each  schedule class
pairsGrpJob = pairsJob.groupBy(lambda x: x[1]).mapValues(len);
pairsGrptasks = pairsTask.groupBy(lambda x: x[1]).mapValues(len);

print("This is for the distribution of the number jobs per scheduling class ")
print(pairsGrpJob.collect())


print("This is for the distribution of the number tasks per scheduling class ")
print(pairsGrptasks.collect())
