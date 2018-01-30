
## Multiprocess External Sorting with Mergesort

### Purpose
When files are too big to fit into memory, external sorting serves as an alternative to large-size file sorting. Support multiple columns sorting.
### Usage
`python ExSorter.py`
### Parameters
param.ini to contral parameters

#### 0 use existing file; 1 use random generated files
> RANDOM_FLAG=0



#### if not extension .csv, all files in input folder will be processed
> FILE_INPUT=merge.csv

#### columns to be sorted in order
> SORT_COLUMNS=0,1,2

#### chunksize for each split file in number of rows
> CHUNK_SIZE=1000000

#### number of cores used
> N_CORES=4


#### 0 keep all the temp files in the temp folder 1 otherwise
> CLEAN_TEMP=1



```python

```
