import sys
from pyspark import SparkContext
import time
import numpy as np


def percent(arr):
    if(len(arr) == 1):
        return 0;

    if(arr[0] < arr[1]):
        return (arr[0]/arr[1])*100
    else:
        return (arr[1]/arr[0])*100

print("******************We started the graded lab**************************")
print("This study is for Do tasks with low priority have a higher probability of being evicted")

sc = SparkContext("local[1]")
data = sc.textFile("./data/task_events/part-00000-of-00500.csv")
lines = data.map(lambda x: x.split(","))

# here what i did is i need the job also to use distinct because if we have 2 tasks example
# (p = 9 , tid =0 , jid = j1) , (p = 9 , tid =0 , jid = j2) distinct will not remove them then
# i again apply map and have (p = 9 , tid =0) (p = 9 , tid =0) both are needed because the belong to different jobs
# eliminate same just from the same jobs
pairsTask = lines.filter(lambda x: (x[8] !="") ).map(lambda x: (int(x[8]) , int(x[3]) , int(x[2])  )).distinct()
pairsTask = pairsTask.map(lambda x: (int(x[0]) , int(x[1]) ))
Total_Tasks_Number = pairsTask.count()
# I have the number tasks belongs to each priority
pairsGrptasks = pairsTask.groupByKey().mapValues(len)
print("--------->distribution of tasks in each priority")
print(pairsGrptasks.sortBy(lambda x: x[0]).collect())

tasks_evicted_per_priority = lines.filter(lambda x:((x[8] !="") & (int(x[5]) == 2))).map(lambda x: (int(x[8]) , int(x[3]) , int(x[2]))).distinct()
tasks_evicted_per_priority = tasks_evicted_per_priority.map(lambda x: (int(x[0]) , int(x[1]) ))
tasks_evicted_per_priority_Grp = tasks_evicted_per_priority.groupByKey().mapValues(len)
print("--------->distribution of tasks evicted per each priority")
print(tasks_evicted_per_priority_Grp.sortBy(lambda x: x[0]).collect())

joinedRdd  = pairsGrptasks.leftOuterJoin(tasks_evicted_per_priority_Grp).map(lambda x: (x[0] , ((x[1][1]/x[1][0]) *100)) if x[1][1] != None else (x[0] , 0.0) )

print ("(result)------->we can see here probability in (%) of tasks evicted from priority 0 is high ")
print (joinedRdd.sortBy(lambda x: x[0]).collect())





print("another implementation using union")
# Second way is to use union and then group by key
joinedRdd  = pairsGrptasks.union(tasks_evicted_per_priority_Grp).groupByKey().mapValues(list).map(lambda x : (x[0] , percent(x[1])))
print(joinedRdd.sortBy(lambda x: x[0]).collect())
