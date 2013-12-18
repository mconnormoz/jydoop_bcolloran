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
    if data[2]=="ACTIVE_IN_RANGE":
        searchProvider = "ACTIVE"
        searchLocation = "ACTIVE"
    elif data[2]=="ANY_SEARCH_PROVIDER":
        searchProvider = "ANY_PROVIDER"
        searchLocation = "ANY_LOCATION"
    else:
        try:
            # print data[2].split("."),data[2].split(".")[0]
            searchProvider = data[2].split(".")[0]
            searchLocation = data[2].split(".")[1]
        except IndexError:
            return

    context.write(tuple(data[:2]),tuple([searchProvider,searchLocation]+data[3:]))


def reduce(key,valIter,context):
    valList = list(valIter)
    activeInRangeInfo = next(val for val in valIter if val[0]=="ACTIVE")
    for val in valList:
        #print key,list(activeInRangeInfo)+list(val)
        context.write(key,list(activeInRangeInfo[2:])+list(val))


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


















