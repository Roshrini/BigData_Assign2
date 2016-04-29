
import re

# ar1=[1,2,3]
# ar2=[1,2,3]
# a=sc.parallelize([1,2,3])
# b=sc.parallelize([1,2,3])
#
# r=a.map(lambda a,b:a+b)
# print(r.take(3))

w="wolf spread ."
r=re.compile("\.")
check=r.match(w)
print(check.string)

