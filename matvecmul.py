from __future__ import division
import pyspark
import sys
import os
import numpy as np
import math
sc = pyspark.SparkContext()

pathA = "/Users/roshaninagmote/Downloads/a2data/a_100x200.txt"
pathB = "/Users/roshaninagmote/Downloads/a2data/x_200.txt"
pathO = "/Users/roshaninagmote/Downloads/a2data/vectorMultiplication.txt"

matA = sc.textFile(pathA)
matB = sc.textFile(pathB)

N = matB.count()
M = int(matA.count() / N)
P = 1

GRP_SIZE = 20
G = N / GRP_SIZE

lineA = matA.map(lambda x: x.split(" "))
lineB = matB.map(lambda x: x.split(" "))

print(lineB.collect())
def map_group_A(item):
    row = item[0]
    col = item[1]
    val = item[2]
    grp = int(math.ceil((float(col.encode('utf8')) + 1) /float(GRP_SIZE)))
    return [((grp),('A', row, col, val))]

def map_group_B(item):
    row = item[1]
    iva = item[0][1] 
    val = iva.rstrip('])')
    grp = int(math.ceil((row + 1) / float(GRP_SIZE)))
    return [((grp),('B',row, 0, val))]

def mult(input):

    a = np.zeros(shape = (M, GRP_SIZE), dtype = np.float64)
    b = np.zeros(shape = (GRP_SIZE, 1), dtype = np.float64)
    
    for elem in input[1][0]:
        i = int(elem[1].encode('utf8'))
        j = int(elem[2].encode('utf8')) % GRP_SIZE
        v = float(elem[3].encode('utf8'))
        a[i][j] = v 
    
    for elem in input[1][1]:
    	i = elem[1] % GRP_SIZE
        j = elem[2]
        v = float(elem[3].encode('utf8'))
        b[i][j] = v

    mul = np.dot(a,b)
    mullist = []
    for x in range(0,M):
        mullist.append((x,mul[x][0]))
    return (mullist)
    
step1 = lineA.flatMap(map_group_A)
step2 = lineB.zipWithIndex().flatMap(map_group_B)
step3 = step1.groupByKey()
step4 = step2.groupByKey()
step5 = step3.join(step4).flatMap(mult).reduceByKey(lambda x,y: x+y).saveAsTextFile(pathO)