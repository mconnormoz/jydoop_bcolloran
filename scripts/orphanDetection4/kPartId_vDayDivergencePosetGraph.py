import jydoop
import healthreportutils
import json
import datetime

import orphUtils
import poset2

output = orphUtils.outputTabSep


setupjob = orphUtils.hdfsjobByType("HDFS")


def sortedDayInfoToDagInfo(sortedDayInfo):
    maxDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    minDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    return [poset2.DayNode(id=label,date=info["date"],minDateInThread=minDateInThread,maxDateInThread=maxDateInThread,data=info["data"]) for label,info in sortedDayInfo]


@orphUtils.localTextInput()
@healthreportutils.FHRMapper()
def map(partId,fhrPayload,context):
    #days are sorted by date by .daily_data()
    sortedDayInfo = [(hash(date+orphUtils.dictToSortedJsonish(data)),
                        {"date":date,"data":data})
                        for date,data in fhrPayload.daily_data()
                        if ('org.mozilla.appSessions.previous' in data.keys())]

    # [str(profileCreation)+"_"+date+"_"+str(hash(str(orphUtils.dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() if ('org.mozilla.appSessions.previous' in payload["data"]["days"][date].keys())]

    if sortedDayInfo:
        context.write(partId,sortedDayInfo)
    else:
        context.getCounter("MAPPER", "unmergable; no day with appSessions").increment(1)

def reduce(partId, iterOfDayInfo, context):
    graphInit = False
    i=0
    dayGraphOut=None
    for sortedDayInfo in iterOfDayInfo:
        # print sortedDayInfo
        print i
        # print sortedDayInfo
        if not graphInit:
            nodes = sortedDayInfoToDagInfo(sortedDayInfo)
            if nodes:
                dayGraphOut = poset2.DayGraph(nodes)
                graphInit=True
            else:
                context.getCounter("REDUCER", "record with 0 days").increment(1)
        else:
            nodes = sortedDayInfoToDagInfo(sortedDayInfo)
            if nodes:
                dayGraphToMerge = poset2.DayGraph(nodes)
                dayGraphOut.merge(dayGraphToMerge)
                context.getCounter("REDUCER", "records merged").increment(1)
        i+=1
    if dayGraphOut:
        # print "dayGraph",dayGraphOut
        # print 'dayGraphOut._possibleMaxEltIds',dayGraphOut._possibleMaxEltIds
        # print 'dayGraphOut.maxElts()',dayGraphOut.maxElts()
        dayGraphOut.addChildTreeWidths()
        dayGraphOut.addWidthOffsets()
        try:
            context.write(partId, str(dayGraphOut))
        except:
            print dayGraphOut.nodeDict
            raise
        context.getCounter("REDUCER", "partsMerged").increment(1)
    else:
        context.getCounter("REDUCER", "PART with 0 days").increment(1)


        







