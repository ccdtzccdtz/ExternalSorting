import numpy as np
import time
import sys,os
from multiprocessing import Pool, Process, Manager
import logging
import multiprocessing
import pandas as pd
import glob
multiprocessing.log_to_stderr(logging.ERROR)

# filenamelist=["%s.csv"%count for count in range(1,5)]
def prepare_random_input(n_file=4,size=10000000,upper=100000000,lower=0,n_col=3):
    count=0
    bigfile=np.random.randint(upper, size=(size,n_col))
    for i in np.split(bigfile, n_file):
        np.savetxt(os.path.join("input","%s.csv"%count),X=i,delimiter=",",fmt='%i')
        count+=1



  #return [np.random.randint(low=lower,high=upper, size=size) for i in range(4) ]

# def split_nfile(bigfile=None,n_file=4,random=False):
#   if random:
#       bigfile=np.random.randint(1000000000, size=(100000000,2))
#   return np.split(bigfile, n_file)

# count=1
# for i in split_nfile(random=True):
#   with open(os.path.join("input","%s.csv"%count),"w") as f:
#       for j in i:
#           f.write(str(j[0])+","+str(j[1])+"\n")
#   count+=1

def split_nfile(bigfile,chunksize=2000000):

    out_num=0

    out_f=open(os.path.join("temp","split_%s.csv"%(out_num)),"w")
    returnlist=["split_%s.csv"%(out_num)]
    i=0
    with open(bigfile,"r") as f:
        
        for row in f:  
            if i==chunksize:
                out_f.close()
                i=-1
                out_num+=1
                out_f=open(os.path.join("temp","split_%s.csv"%(out_num)),"w")
                returnlist.append("split_%s.csv"%(out_num))
                
            out_f.write(row)
            i+=1
    return returnlist

def compareV(i1,i2,v1,v2,ind,col):
    assert ind<len(col)
    while True: 
        l1,l2=v1.rstrip("\n").split(","),v2.rstrip("\n").split(",")
        if int(l1[col[ind]]) < int(l2[col[ind]]):
            ind=0
            yield v1
            try:
                v1 = next(i1)
            except StopIteration:
                yield v2
                while True:
                    yield next(i2)
        elif int(l1[col[ind]]) == int(l2[col[ind]]):
            ind+=1
            compareV(i1,i2,v1,v2,ind,col)
        else:
            ind=0
            yield v2
            try:
                v2 = next(i2)
            except StopIteration:
                yield v1
                while True:
                    yield next(i1)


def readfile(filename):
    with open(os.path.join("temp",filename),"r") as f:
        for line in f:
            # if NUM_COL==1:
            #     yield int(line.rstrip("\n"))
            # if NUM_COL==2:
            yield [int(i.rstrip("\n")) for i in line.split(",")]
                

def sorter(*arg):

    filename,col=arg
    print("working on process %s" %os.getpid())
    listA=[line for line in readfile(filename)]
    try:
        fname=filename.split("_")[1]
    except:
        fname=filename.split(os.path.sep)[-1]
    arr=np.array(listA)
    ind=np.lexsort([arr[:,i] for i in reversed(col)])
    np.savetxt(os.path.join("temp",fname),X=arr[ind],delimiter=",",fmt='%i')
        # for i,j in arr[ind]:
        #         f.write(str(i)+","+str(j)+"\n")

def sort_manager(n_core=4,col=[0]):
    p = Pool(n_core)
    p.starmap(sorter, [(file,col) for file in filenamelist])
    print(filenamelist)
    p.close()
    p.join()


def writecsv(iterA,filename):
    with open(filename,"w") as f:
        for i in iterA:
            f.write(i)

def merge_multi(f1name,f2name,res):
    if f2name==0:
        res.append(f1name)

    else:
        fname_combine=f1name.split(".")[0]+"_"+f2name.split(".")[0]+".csv"
        fname=os.path.join("temp",fname_combine)
        print(fname)
        f1=open(os.path.join("temp",f1name),"r")
        f2=open(os.path.join("temp",f2name),"r")
        v1,v2=next(f1),next(f2)
        ind=0
        col_order=[0,1]
        writecsv(compareV(f1,f2,v1,v2,ind,col_order),fname)
        res.append(fname_combine)

if __name__ == '__main__':

    random_flag=1
    file_arg="merge.csv"
    col=[0,1,2]


    if random_flag==1:
        prepare_random_input()
    start=time.time()
    if len(file_arg.split('.'))>1:   # if the argument is a single file
        bigfile=os.path.join("input",file_arg)
        filenamelist=split_nfile(bigfile)
    else: # if the argument is a directory
        filenamelist=glob.glob(os.path.join(os.getcwd(),"input","*.csv"))
        print(filenamelist)
    

    end=time.time()
    print("Total Run Time: %s s"%(end-start))   

    start=time.time()     

    resultlist=sort_manager(col)

    end=time.time()
    print("Total Run Time: %s s"%(end-start)) 

    start=time.time()

    manager = Manager() 
    responses = manager.list()

    for fname in filenamelist:
        fn=fname.split("_")[1]
        responses.append(fn)

    while len(responses) > 1:
        p = []
        while len(responses) > 0:
            if len(responses)==1:
                proc = Process( target=merge_multi, args=(responses.pop(0),0,responses) )
            else:
                proc = Process( target=merge_multi, args=(responses.pop(0),responses.pop(0),responses) )
            p.append( proc )
            print("Current length: %s"%(len(responses)))
        for proc in p:
                proc.start()
        for proc in p:
                proc.join()
                proc.terminate()
    end=time.time()
    print("Total Run Time: %s s"%(end-start))