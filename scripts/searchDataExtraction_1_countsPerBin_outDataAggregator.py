import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv


#setupjob = healthreportutils.setupjob

setupjob = sequencefileutils.setupjob

def map(infileRowNum, row, context):
    data = row.strip().split(",")
    context.write(tuple(data[:3]),tuple(data[3:]))


def reduce(key,valIter,context):
    valList = list(valIter)
    anyProviderInfo = next(val for val in valIter if val[0]=="ACTIVE_IN_RANGE")
    for val in valList:
        #print key,list(anyProviderInfo)+list(val)
        context.write(key,list(anyProviderInfo[1:])+list(val))


# def output(path,reducerOutput):
#     """
#     Output key/values into a reasonable CSV.

#     All lists/tuples are unwrapped.
#     """
#     f = open(path, 'w')
#     w = csv.writer(f,quoting=csv.QUOTE_ALL)
#     for k, v in reducerOutput:
#         l = []
#         jydoop.unwrap(l, k)
#         # unwrap(l, v)
#         w.writerow(l+[str(dict(v))])


















