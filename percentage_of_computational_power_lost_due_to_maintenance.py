import sys
from pyspark import SparkContext
import time
import numpy as np


import sys
from pyspark import SparkContext
import time
import numpy as np
def upPower(arr , maxTime ):
    powerup = 0.0
    # sorting array according to time
    sorted(arr, key=lambda x: x[0])
    prev_time =arr[0][0]
    i = 0
    flag = 0
    while i < len(arr):
        if(arr[i][2] == 0 ):
             prev_time = arr[i][0]
        if(arr[i][2]== 1):
            flag = 1
            remainT =arr[i][0] - prev_time
            powerup = powerup +  remainT*arr[i][1]
            # print(remainT*arr[i][1])
        i  = i + 1
    #case if this machine is alive from begining and it is not maintained
    # print(flag)
    # if(flag == 0):
    #     powerup = maxTime * arr[0][1]
    return powerup

def downPower(arr):
    powerDown = 0.0
    # Sorting the array in increasing order
    sorted(arr, key=lambda x: x[0])
    prev_time =0
    i = 0
    firstTime =  1;

    while i < len(arr):
        if(arr[i][2] == 1 ):
             prev_time = arr[i][0]
             #first time used as a flag to avoid the first add of the machine
        if(arr[i][2]== 0 and firstTime !=1):
            remainT =arr[i][0] - prev_time
            powerDown = powerDown +  remainT*arr[i][1]
            # print(remainT*arr[i][1])

        if(firstTime == 1 ):
            firstTime = 0
        i  = i + 1
    return powerDown

print("******************We started the graded lab**************************")
print("This study is for:percentage of machines that fail down during all the period ")

sc = SparkContext("local[1]")
data = sc.textFile("./data/machine_events/part-00000-of-00001.csv")
lines = data.map(lambda x: x.split(","))

# get the highest time
maxTime = lines.map(lambda x: int(x[0])).sortBy(lambda x: x).sortBy(lambda x: x , ascending= False).collect()[0]

# ( machine ID, ( time , cpu amount,event type))
All = lines.filter(lambda x: (x[4] != "") ).map(lambda x: ( int(x[1]) , (int(x[0]) , float(x[4]) ,int(x[2])) ))
All = All.groupByKey().mapValues(list)


# calculate all power down and up
ExecuteDown = All.map(lambda x : (x[0] , downPower(x[1])))
ExecuteUp = All.map(lambda x : (x[0] , upPower(x[1]  , maxTime)))

totalTimeUP = ExecuteUp.map(lambda x : x[1]).fold(0,lambda a,b: a+b );
totalTimeDown = ExecuteDown.map(lambda x : x[1]).fold(0,lambda a,b: a+b );
# print(totalTimeDown , totalTimeUP)

print("The average of computational power lost due to maintenance" , (totalTimeDown/(totalTimeUP + totalTimeDown))*100 , "% ")
