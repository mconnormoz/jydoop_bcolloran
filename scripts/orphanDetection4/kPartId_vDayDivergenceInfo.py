import jydoop
import healthreportutils
import json
import datetime

import orphUtils
import poset

output = orphUtils.outputTabSep


setupjob = orphUtils.hdfsjobByType("HDFS")


def sortedDayInfoToDagInfo(sortedDayInfo):
    maxDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    minDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    return [poset.DayNode(id=label,date=info["date"],minDateInThread=minDateInThread,maxDateInThread=maxDateInThread,data=info["data"]) for label,info in sortedDayInfo]


@orphUtils.localTextInput()
@healthreportutils.FHRMapper()
def map(partId,fhrPayload,context):
    sortedDayInfo = [(hash(date+orphUtils.dictToSortedJsonish(data)),
                        {"date":date,"data":data})
                        for date,data in fhrPayload.daily_data()]
    context.write(partId,sortedDayInfo)

def reduce(partId, iterOfDayInfo, context):
    graphInit = False
    i=0
    dayGraph=None
    for sortedDayInfo in iterOfDayInfo:
        # print sortedDayInfo
        print i
        if not graphInit:
            nodes = sortedDayInfoToDagInfo(sortedDayInfo)
            # print nodes
            if nodes:
                dayGraphOut = poset.Poset(nodes)
                graphInit=True
            else:
                context.getCounter("REDUCER", "record with 0 days").increment(1)
        else:
            nodes = sortedDayInfoToDagInfo(sortedDayInfo)
            if nodes:
                dayGraphToMerge = poset.Poset(nodes)
                dayGraphOut.merge(dayGraphToMerge)
                context.getCounter("REDUCER", "records merged").increment(1)
        i+=1
    if dayGraphOut:
        # print "dayGraph",dayGraphOut
        print 'dayGraph._possibleMaxEltIds',dayGraphOut._possibleMaxEltIds
        print 'dayGraph.maxElts()',dayGraphOut.maxElts()
        context.write(partId, (i,len(dayGraphOut.minElts()),len(dayGraphOut.maxElts())) )
        context.getCounter("REDUCER", "partsMerged").increment(1)
    else:
        context.getCounter("REDUCER", "PART with 0 days").increment(1)


        







