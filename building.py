import sys
from pyspark import SparkContext
import time
import numpy as np

def check(arr):
    if(len(arr) == 1):
        return True;
    i = 0

    while(i < len(arr) - 1):
        if(arr[i][1] != arr[i+1][1] ):
            return False
        i = i + 1
    return False

print("******************We started the graded lab**************************")
print("This study is for: do tasks from the same job run on the same machine ? ")

sc = SparkContext("local[1]")
data = sc.textFile("./data/task_events/part-00000-of-00500.csv")
header = data.collect()[0].replace('"','').split(',')
lines = data.map(lambda x: x.split(","))

# (x[5] == "1") to pick only the scheduled tasks
# also remove the redandunt (if exist)
pairs = lines.filter(lambda x: ((x[4] != "") & (x[5] == "1"))  ).map(lambda x : (int(x[2]) ,(int(x[3]) ,int(x[4])))).distinct()
# print(pairs.collect())

pairs = pairs.groupByKey().mapValues(list)
# print(pairs.collect())

pairs = pairs.map(lambda x: (x[0] , check(x[1])))
print(pairs.collect())
print("using fold we can have at the end False result of the above data (result = pairs.map(lambda x : x[1]).fold(True,lambda a,b:  np.logical_and(a, b) );) ")
result = pairs.map(lambda x : x[1]).fold(True,lambda a,b:  np.logical_and(a, b) );
print("After my analysis is done result says:", result , " it tasks from the same job can be disitributed over multiple machines")
