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
                dayGraphOut.merge(dayGraphToMerge)
                context.getCounter("REDUCER", "records merged").increment(1)
        else:
            context.getCounter("REDUCER", "record with no appSessions").increment(1)

    if dayGraphOut:
        numHeadRecords = len(dayGraphOut.maxElts())
        context.write(partId, [numRecordsThisPart,numHeadRecords])
        context.getCounter("REDUCER", "partsMerged").increment(1)
    else:
        context.getCounter("REDUCER", "PART with no appSessions").increment(1)


        







