import numpy.random as nprnd

a=[1,4,5,7]
b=[2,3,6,8]
c=[3,6,11,12]

def merge(listA,listB):
    i1,i2=iter(listA),iter(listB)
    v1,v2=next(i1),next(i2)
    while True:
        if v1 < v2:
            yield v1
            try:
                v1 = next(i1)
            except StopIteration:
                yield v2
                while True:
                    yield next(i2)
        else:
            yield v2
            try:
                v2 = next(i2)
            except StopIteration:
                yield v1
                while True:
                    yield next(i1) 
for i in merge(merge(a,b),c):
	print(i) 


