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
print("This study is for:percentage of machines that fail down during all the period ")

sc = SparkContext("local[1]")
data = sc.textFile("./data/machine_events/part-00000-of-00001.csv")
lines = data.map(lambda x: x.split(","))


sumOfDisconnectedMachinesCpus = lines.filter(lambda x: (x[4] != "") & (int(x[2]) == 1)).map(lambda x: int(x[1])).distinct().count()
# totalSum = lines.filter(lambda x: (x[4] != "") ).map(lambda x: float(x[4])).fold(0,lambda a,b: a+b )
# This way in total sum I'm calculating lets say for machine of id =1 sum all of types
# 1 event type = 0 0.5
# 1 event type = 1 0.5
# 1 event type = 0 0.5                      sum += 0.5+0.5+0.5 WRONG I need to take it only once


# here what is done is transforming to pairs ( k ,v) and eliminate all the redantunt pairs
totalSum = lines.filter(lambda x: (x[4] != "") ).map(lambda x: ( int(x[1]), float(x[4])) ).distinct().map(lambda x: x[0]).distinct().count()

#but the loss is very big i think something os wrong :?>

print("this is the sum of disconnected machines")
print(sumOfDisconnectedMachinesCpus)
print("this is the total sum ")
print(totalSum)
print("the percentage of machines that removed due to maintanece  =")
print((((sumOfDisconnectedMachinesCpus)/(totalSum))*100))
