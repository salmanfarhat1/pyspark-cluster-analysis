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
print("This study is for: distribution of the machines according to their CPU capacity")

sc = SparkContext("local[1]")
data = sc.textFile("./data/machine_events/part-00000-of-00001.csv")
lines = data.map(lambda x: x.split(","))

#remove lines where the cpu normalized quantity empty
pairs = lines.filter(lambda x:(x[4] != "")).map(lambda x: ( int(x[1]), float(x[4]))).distinct()
# print(pairs.collect())


# Group by value then count the list that is grp
pairsgrp = pairs.groupByKey().mapValues(len);
# print(pairsgrp.collect())

# We can check the value of a pair if it is not = 1 then there is an error in the data because if
# it is not equal 1 then this machine has 2 different cpu capacity
# (but is it the case  of removing the machine then adding it ? good question ask professor thomas ropars)



pairsgrp = pairs.groupBy(lambda x: x[1]).mapValues(len);
print(pairsgrp.collect())
