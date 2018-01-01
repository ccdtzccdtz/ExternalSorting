import numpy as np
import time
import sys,os
from multiprocessing import Pool



def prepare_random_input(n_file=4,size=1000000,upper=100000000,lower=0):
	return [np.random.randint(low=lower,high=upper, size=size) for i in range(4) ]

def split_nfile(bigfile=None,n_file=4,random=False):
	if random:
		bigfile=np.random.randint(1000000000, size=100000000)
	return np.split(bigfile, n_file)

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

def sorter(listA):
	return np.sort(listA)

def writecsv(iterA,filename):
	with open(filename,"w") as f:
		for i in iterA:
			f.write(str(i)+"\n")

#a,b,c,d=prepare_random_input()
a,b,c,d=split_nfile(random=True)
print("split successfully!")

p = Pool(4)
resultlist=p.map(sorter, [a,b,c,d])
print(resultlist)

# filename=os.path.join("output","result.csv")
# start=time.time()
# writecsv(merge(merge(sorter(a),sorter(b)),merge(sorter(c),sorter(d))),filename)
# end=time.time()
# print(end-start)
