from __future__ import division
import pyspark
import sys
import os
import collections
import numpy as np
import math
import itertools

M=200
N=200
P=200

path = '/Users/roshaninagmote/Downloads/a2_graphs/Assign2_200.txt'
newPath = '/Users/roshaninagmote/Downloads/a2_graphs/Assign2_200_new.txt'
finalPath = '/Users/roshaninagmote/Downloads/a2_graphs/Assign2_200_multi.txt'

sc = pyspark.SparkContext()
GRP_SIZE=20
temp_listoflist=[]
temp_list=[]
def load_matrix(fname, sz):
    M = np.matrix(np.zeros(sz))
    with open(fname) as f:
        content = f.readlines()
        for l in content:
            s = l.split()
            M[int(s[0]), int(s[1])] = float(s[2])
            temp_list=[]
            temp_list.append(int(s[0]))
            temp_list.append(int(s[1]))
            temp_list.append(float(s[2]))
            temp_listoflist.append(temp_list)
    return temp_listoflist

A = load_matrix(path,(M, N))

mylist = []
myBigList = []
for i in range(0,M):
    for j in range(0,N):
        mylist = []
        mylist.append(i)
        mylist.append(j)
        mylist.append(0)
        myBigList.append(mylist)

for i in range(0,len(A)):
            j=A[i][1]
            while(A[i][0]!=myBigList[j][0]):
                j=j+N
            myBigList[j][2]=1

f = open(newPath, 'w')

for item in myBigList:
  f.write("%s\n" % ' '.join(map(str,item)))

f.close()

matA=sc.textFile(newPath)
matB=sc.textFile(newPath)

def group_mapper_A(item):
    row = item[0]
    col = item[1]
    val = item[2]

    i_grp = int(math.ceil((float(row.encode('utf8'))+1 )/ float(GRP_SIZE)))
    return [((i_grp, k), ('A', row, col, val) ) for k in range(1, int(float(N/GRP_SIZE)+1))]

def group_mapper_B(item):
    row = item[0]
    col = item[1]
    val = item[2]

    k_grp = int(math.ceil((float(col.encode('utf8'))+1) / float(GRP_SIZE)))
    return [( (i,k_grp ), ('B', row, col, val) ) for i in range(1,int(float(N/GRP_SIZE+1)))]

final={}

def mult(input):
    a = np.zeros(shape=(GRP_SIZE, N), dtype=np.float64)
    b = np.zeros(shape=(N, GRP_SIZE), dtype=np.float64)

    for elem in input[1][0]:
        ar = int(elem[1].encode('utf8'))
        ac = int(elem[2].encode('utf8'))
        ar1=ar%GRP_SIZE;
        a[ar1][ac]=float(elem[3].encode('utf8'))

    for elem in input[1][1]:
        br=int(elem[1].encode('utf8'))
        bc=int(elem[2].encode('utf8'))
        bc1=bc%GRP_SIZE;
        b[br][bc1]=float(elem[3].encode('utf8'))

    r=c=0;
    r=r+(input[0][0]-1)*GRP_SIZE;
    c=c+(input[0][1]-1)*GRP_SIZE;

    mul= np.dot(a,b)
    mul_list=mul.tolist()
    final[(r,c)]=mul_list
    return final

lineA=matA.map(lambda x: x.split(" "))
lineB=matB.map(lambda x: x.split(" "))

A_groups = lineA.flatMap(group_mapper_A)
B_groups = lineB.flatMap(group_mapper_B)

A_grp = A_groups.groupByKey()
B_grp = B_groups.groupByKey()

a_join = A_grp.join(B_grp)
a=a_join.map(mult).collect()
listoflists=[]
list1=[]

GA=int((M*P)/(GRP_SIZE*GRP_SIZE))

for i in range (0,GA):
    for key1, value1 in a[i].iteritems():
        for r in range(0, GRP_SIZE):
            for c in range(0,GRP_SIZE):
                list1=[]
                list1.append(key1[0]+r)
                list1.append(key1[1]+c)
                list1.append(int(value1[r][c]))
                listoflists.append(list1)

listoflists.sort()
unique_list=list(listoflists for listoflists,_ in itertools.groupby(listoflists))

f1 = open(finalPath, 'w')

for item in unique_list:
    f1.write("%s\n" % ' '.join(map(str,item)))

f1.close()

matA2 = sc.textFile(finalPath)

def format_fn(input):
    return(int(input[0]),int(input[1]),int(input[2]))

linea=matA.map(lambda x: x.split(" "))
lineA1=linea.map(format_fn).map(lambda (a,b,c):((a,b),c))
lineA2=matA2.map(lambda x: x.split(" ")).map(format_fn).map(lambda (a,b,c):((a,b),c))
finalans=lineA1.union(lineA2).reduceByKey(lambda x,y:x+y).collect()

shallow = 'true'
for i in range(0,len(finalans)):
    if(finalans[i][1] == 0 and ((finalans[i][0][0] != finalans[i][0][1]) and (finalans[i][0][0] + finalans[i][0][1] != (N-1)))):
        print("answer",finalans[i][0],finalans[i][1])
        shallow= 'false';
        break;

print(shallow)