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
print("This study the percentage of jobs/tasks that got killed or evicted depending on the scheduling class")

sc = SparkContext("local[1]")
data = sc.textFile("./data/task_events/part-00000-of-00500.csv")

lines = data.map(lambda x: x.split(","))



#I could switch (int(x[2]) , int(x[7]) and use gruopbykey but it is on purpose to use gruopBy(....)
#removing all the duplicates from the same job
pairsJob = lines.filter(lambda x: (x[7] !="") ).map(lambda x: (int(x[2]) , int(x[7] ))).distinct()
pairsTask = lines.filter(lambda x: (x[7] !="") ).map(lambda x: (int(x[3]) , int(x[7])  , int(x[2]))).distinct()
pairsTask = pairsTask.map(lambda x: (x[0] , x[1]))

#now groupby the value and calculate number of tids / jids that we have per each  schedule class
pairsGrpJob = pairsJob.groupBy(lambda x: x[1]).mapValues(len)
pairsGrptasks = pairsTask.groupBy(lambda x: x[1]).mapValues(len)


# keep only tasks Id's with schedule class
numberOfTasks_E_K = lines.filter(lambda x:(x[7] !="")  & ((int(x[5]) == 2) | (int(x[5]) == 5)) ).map(lambda x: (int(x[7]) , int(x[3]) , int(x[2])))
numberOfTasks_E_K = numberOfTasks_E_K.map(lambda x:(x[0] , x[1])).groupByKey().mapValues(len).sortByKey()
# No need for sorting just for better viewing
print("This is for the distribution of the number tasks per scheduling class that are evicted or killed ")
print(numberOfTasks_E_K.sortByKey().collect())


print("This is for the distribution of the number tasks per scheduling class ")
print(pairsGrptasks.sortByKey().collect())

# First method
joinedRdd  = pairsGrptasks.union(numberOfTasks_E_K).groupByKey().mapValues(list).map(lambda x : (x[0] , percent(x[1])))
print(joinedRdd.sortByKey().collect())



# second method
my_task_dict = dict(pairsGrptasks.collect())
numberOfTasks_E_K_dict = dict(numberOfTasks_E_K.collect())

# print (numberOfTasks_E_K_dict)

print ("percentage of evicted or killed in each scheduling class is")


class3 = (numberOfTasks_E_K_dict[3]/my_task_dict[3])*100
class2 = (numberOfTasks_E_K_dict[2]/my_task_dict[2])*100
class1 = (numberOfTasks_E_K_dict[1]/my_task_dict[1])*100
class0 = (numberOfTasks_E_K_dict[0]/my_task_dict[0])*100
print ("scheduling class 0 : ", class0,"%")
print ("scheduling class 1 : ", class1,"%")
print ("scheduling class 2 : ", class2,"%")
print ("scheduling class 3 : ", class3,"%")
# PS: do it for jobs


# print(pairsGrptasks.collect()[0][1])













# import sys
# from pyspark import SparkContext
# import time
# import numpy as np
#
#
# print("******************We started the graded lab**************************")
# print("This study the percentage of jobs/tasks that got killed or evicted depending on the scheduling class")
#
# sc = SparkContext("local[1]")
# data = sc.textFile("./data/task_events/part-00000-of-00500.csv")
#
# lines = data.map(lambda x: x.split(","))
#
#
#
# #I could switch (int(x[2]) , int(x[7]) and use grp by key but it is on purpose to use gruopBy(....)
# #removing all the duplicates
# pairsJob = lines.filter(lambda x: (x[7] !="") ).map(lambda x: (int(x[2]) , int(x[7]))).distinct()
# pairsTask = lines.filter(lambda x: (x[7] !="") ).map(lambda x: (int(x[3]) , int(x[7]))).distinct()
#
# #now groupby the value and calculate number of tids / jids that we have per each  schedule class
# pairsGrpJob = pairsJob.groupBy(lambda x: x[1]).mapValues(len)
# pairsGrptasks = pairsTask.groupBy(lambda x: x[1]).mapValues(len)
#
#
# # keep only tasks Id's with schedule class
# numberOfTasks_E_K = lines.filter(lambda x:(x[7] !="")  & ((int(x[5]) == 2) | (int(x[5]) == 5)) ).map(lambda x: (int(x[7]) , int(x[3]))).distinct()
# numberOfTasks_E_K = numberOfTasks_E_K.groupByKey().mapValues(len).sortByKey()
# # No need for sorting just for better viewing
# print("This is for the distribution of the number tasks per scheduling class that are evicted or killed ")
# print(numberOfTasks_E_K.collect())
#
# # print("This is for the distribution of the number jobs per scheduling class ")
# # print(pairsGrpJob.collect())
# #
# #
# print("This is for the distribution of the number tasks per scheduling class ")
# print(pairsGrptasks.sortByKey().collect())
#
#
#
# my_task_dict = dict(pairsGrptasks.collect())
# numberOfTasks_E_K_dict = dict(numberOfTasks_E_K.collect())
#
# # print (numberOfTasks_E_K_dict)
#
# print ("percentage of evicted or killed in each scheduling class is")
#
#
# class3 = (numberOfTasks_E_K_dict[3]/my_task_dict[3])*100
# class2 = (numberOfTasks_E_K_dict[2]/my_task_dict[2])*100
# class1 = (numberOfTasks_E_K_dict[1]/my_task_dict[1])*100
# class0 = (numberOfTasks_E_K_dict[0]/my_task_dict[0])*100
# print ("scheduling class 0 : ", class0,"%")
# print ("scheduling class 1 : ", class1,"%")
# print ("scheduling class 2 : ", class2,"%")
# print ("scheduling class 3 : ", class3,"%")
# # PS: do it for jobs
#
#
# # print(pairsGrptasks.collect()[0][1])
