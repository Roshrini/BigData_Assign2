from __future__ import division
from operator import add
import pyspark
import sys
import os
import collections
import numpy as np
import math
import itertools

sc = pyspark.SparkContext()

matA=sc.textFile("/Users/roshaninagmote/Downloads/a2_graphs/assign2_100_1.txt")
matB=sc.textFile("/Users/roshaninagmote/Downloads/a2data/b_200x100.txt")

#matA=sc.textFile("/Users/roshaninagmote/Documents/a.txt")
#matB=sc.textFile("/Users/roshaninagmote/Documents/b.txt")

matA_cnt=matA.count()
matB_cnt=matB.count()

M=100
N=200
P=100

GRP_SIZE=20
# M=2
# N=3
# P=2
G=M/GRP_SIZE

def group_mapper_A(item):
    row = item[0]
    col = item[1]
    val = item[2]

    i_grp = int(math.ceil((float(row.encode('utf8'))+1 )/ float(GRP_SIZE)))
    return [((i_grp, k), ('A', row, col, val) ) for k in range(1, 6)]

def group_mapper_B(item):
    row = item[0]
    col = item[1]
    val = item[2]

    k_grp = int(math.ceil((float(col.encode('utf8'))+1) / float(GRP_SIZE)))
    return [( (i,k_grp ), ('B', row, col, val) ) for i in range(1,6)]

dictA=collections.defaultdict(list)
dictB=collections.defaultdict(list)
final={}

def mult(input):
    a = np.zeros(shape=(GRP_SIZE, N), dtype=np.float64)
    b = np.zeros(shape=(N, GRP_SIZE), dtype=np.float64)

    for elem in input[1][0]:
        ar = int(elem[1].encode('utf8'))
        ac = int(elem[2].encode('utf8'))
        ar1=ar%GRP_SIZE;
        a[ar1][ac]=float(elem[3].encode('utf8'))
        dictA[ar].append(elem[3].encode('utf8'))

    for elem in input[1][1]:
        br=int(elem[1].encode('utf8'))
        bc=int(elem[2].encode('utf8'))
        bc1=bc%GRP_SIZE;
        b[br][bc1]=float(elem[3].encode('utf8'))
        dictB[bc].append(elem[3].encode('utf8'))

    r=c=0;
    r=(input[0][0]-1)*GRP_SIZE;
    c=(input[0][1]-1)*GRP_SIZE;

    mul= np.dot(a,b)
    mul_list=mul.tolist()
    final[(r,c)]=mul_list
    return final

lineA=matA.map(lambda x: x.split(" "))
lineB=matB.map(lambda x: x.split(" "))
#print(lineA.collect())
# def check(inp):
# 	print(inp)
#
# (m,n) = lineA.map(check).collect()
print("linea",matA.count())
(m,n)  = lineA.map(lambda s: (int(s[0]), int(s[1]))).reduce(lambda a,b: map(max, zip(a,b)))
print("m,n",m,n)
#assert m==n
block_size = 50

def fill_block(B, x):
    B[x[0]%block_size, x[1]%block_size] = x[2]
    return B

A = lineA.map(lambda s: ( (int(s[0])/block_size, (int(s[1]))/block_size), (int(s[0]), int(s[1]), float(s[2])))) \
         .aggregateByKey(np.matrix(np.zeros((block_size, block_size))), fill_block, add).cache()

def is_full(mat):
    return np.count_nonzero(mat) == np.prod(mat.shape)

# lets check if A is full ...
ch=A.values().map(is_full).reduce(lambda x,y: x & y)
print("A",A.count())

#
# A_groups = lineA.flatMap(group_mapper_A)
# B_groups = lineB.flatMap(group_mapper_B)
#
# A_grp = A_groups.groupByKey()
# B_grp = B_groups.groupByKey()
#
# a_join = A_grp.join(B_grp)
# a=a_join.map(mult).collect()
#
# listoflist=[]
# list1=[]
# G=int((M*P)/(GRP_SIZE*GRP_SIZE))
#
# for i in range (0,G):
#     for key1, value1 in a[i].iteritems():
#         for r in range(0, GRP_SIZE):
#             for c in range(0,GRP_SIZE):
#                 list1=[]
#                 list1.append(key1[0]+r)
#                 list1.append(key1[1]+c)
#                 list1.append(value1[r][c])
#                 listoflist.append(list1)
#
# listoflist.sort()
# unique_list=list(listoflist for listoflist,_ in itertools.groupby(listoflist))
#
# f = open('/Users/roshaninagmote/Documents/workfile', 'w')
# for item in unique_list:
#   f.write("%s\n" % ' '.join(map(str,item)))
#
# f.close()