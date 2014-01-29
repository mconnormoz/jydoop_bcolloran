import orphUtils
import poset2
import hashlib

output = orphUtils.outputTabSep

######## to OUTPUT TO HDFS
def skip_local_output():
    return True


setupjob = orphUtils.hdfsjobByType("JYDOOP")


'''
sortedDayInfo will arrive as a tuple of strings like:
15716_2013-08-08_3883818952507600032
15716_2013-08-10_-8620927156736977068
e.g.,
{prof creation date}_{date of data$days entry}_{hash of data$days entry}

'''


def sortedDayInfoToDagInfo(sortedDayInfoStrings):
    #this will return a list of tuples: (profCreatDate,date,hashStr)
    sortedDayInfo = [tuple(s.split("_")) for s in sortedDayInfoStrings]
    # print type(sortedDayInfoStrings)
    # print sortedDayInfoStrings
    # print sortedDayInfo

    maxDateInThread = max( tup[1] for  tup in sortedDayInfo)
    minDateInThread = min( tup[1] for  tup in sortedDayInfo)
    return [poset2.DayNode(id=hashlib.md5(s).hexdigest(),
                            date=s.split("_")[1],
                            minDateInThread=minDateInThread,
                            maxDateInThread=maxDateInThread,
                            data=None) for s in sortedDayInfoStrings]


@orphUtils.localTextInput(evalTup=True)
def map(partId,sortedDayInfo,context):
    # print type(sortedDayInfo)
    context.write(partId,sortedDayInfo)


def reduce(partId, iterOfDayInfo, context):
    graphInit = False
    numRecordsThisPart = 0
    dayGraphOut=None
    for sortedDayInfo in iterOfDayInfo:
        numRecordsThisPart+=1
        if sortedDayInfo:
            if not graphInit:
                nodes = sortedDayInfoToDagInfo(sortedDayInfo)
                dayGraphOut = poset2.DayGraph(nodes)
                graphInit=True
            else:
                nodes = sortedDayInfoToDagInfo(sortedDayInfo)
                dayGraphToMerge = poset2.DayGraph(nodes)
                try:
                    dayGraphOut.merge(dayGraphToMerge)
                except:
                    context.getCounter("REDUCER", "parts FAILED to merge").increment(1)
                    return
                context.getCounter("REDUCER", "records merged").increment(1)
        else:
            context.getCounter("REDUCER", "record with no appSessions").increment(1)

    if dayGraphOut:
        numHeadRecords = len(dayGraphOut.maxElts())
        context.write(partId, (numRecordsThisPart,numHeadRecords))
        context.getCounter("REDUCER", "partsMerged").increment(1)
    else:
        context.getCounter("REDUCER", "PART with no appSessions").increment(1)


        







