import numpy as np
import time
import sys,os
from multiprocessing import Pool, Process, Manager
import logging
import multiprocessing
import uuid
#multiprocessing.log_to_stderr(logging.DEBUG)


filenamelist=["%s.csv"%count for count in range(1,5)]
# def prepare_random_input(n_file=4,size=1000000,upper=100000000,lower=0):
#   return [np.random.randint(low=lower,high=upper, size=size) for i in range(4) ]

# def split_nfile(bigfile=None,n_file=4,random=False):
#   if random:
#       bigfile=np.random.randint(1000000000, size=100000000)
#   return np.split(bigfile, n_file)

# count=1
# for i in split_nfile(random=True):
#   with open(os.path.join("input","%s.csv"%count),"w") as f:
#       for j in i:
#           f.write(str(j)+"\n")
#   count+=1


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
def mergefile(i1,i2):
    #i1,i2=iter(listA),iter(listB)
    v1,v2=next(i1),next(i2)
    while True:
        if v1 < v2:
            yield v1.rstrip("\n")
            try:
                v1 = next(i1)
            except StopIteration:
                yield v2.rstrip("\n")
                while True:
                    yield next(i2).rstrip("\n")
        else:
            yield v2.rstrip("\n")
            try:
                v2 = next(i2)
            except StopIteration:
                yield v1.rstrip("\n")
                while True:
                    yield next(i1).rstrip("\n")
def readfile(filename):
    with open(os.path.join("input",filename),"r") as f:
        for line in f:
            yield int(line.strip("\n"))

def sorter(filename):
    print("working on process %s" %os.getpid())
    listA=[line for line in readfile(filename)]
    with open(os.path.join("temp",filename),"w") as f:
        for i in np.sort(listA):
            f.write(str(i)+"\n")


def sort_manager():
    p = Pool(3)
    p.map(sorter, filenamelist)
    p.close()
    p.join()


def writecsv(iterA,filename):
    with open(filename,"w") as f:
        for i in iterA:
            f.write(str(i)+"\n")

def merge_multi(f1name,f2name,res):
    fname_combine=f1name.split(".")[0]+"_"+f2name.split(".")[0]+".csv"
    fname=os.path.join("temp",fname_combine)
    f1=open(os.path.join("temp",f1name),"r")
    f2=open(os.path.join("temp",f2name),"r")
    writecsv(mergefile(f1, f2),fname)
    res.append(fname_combine)

if __name__ == '__main__':
    # #a,b,c,d=prepare_random_input()
    
    # print("split successfully!")
    #sorter("1.csv")
    #resultlist=sort_manager()
    # f1= open(os.path.join("temp","1.csv"),"r")
    # f2= open(os.path.join("temp","2.csv"),"r")
    # f3= open(os.path.join("temp","3.csv"),"r")
    # f4= open(os.path.join("temp","4.csv"),"r")
    f1="1.csv"
    f2="2.csv"
    f3="3.csv"
    f4="4.csv"
    manager = Manager() 
    responses = manager.list()
    responses.append(f1)
    responses.append(f2)
    responses.append(f3)
    responses.append(f4)

    p = []
    if len(responses) > 2:
      while len(responses) > 0:
        proc = Process( target=merge_multi, args=(responses.pop(0),responses.pop(0),responses) )
        p.append( proc )
      for proc in p:
        proc.start()
      for proc in p:
        proc.join()
    f = merge_multi(responses[0], responses[1],responses)   


    # for i in mergefile(f1,f2):
    #   print(i)




#   for i in resultlist:
#       print(i)



    #print(resultlist)

# filename=os.path.join("output","result.csv")
# start=time.time()
# writecsv(merge(merge(sorter(a),sorter(b)),merge(sorter(c),sorter(d))),filename)
# end=time.time()
# print(end-start)
