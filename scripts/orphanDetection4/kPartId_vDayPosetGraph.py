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
        nodes = sortedDayInfoToDagInfo(sortedDayInfo)
        dayGraphOut = poset2.DayGraph(nodes)
        dayGraphOut.addChildTreeWidths()
        dayGraphOut.addWidthOffsets()
        
        context.write(partId,dayGraphOut)
        context.getCounter("MAPPER", "day chain out").increment(1)
    else:
        context.getCounter("MAPPER", "no days with appSessions").increment(1)

# def reduce(partId, iterOfDayInfo, context):
#     graphInit = False
#     i=0
#     dayGraphOut=None
#     for sortedDayInfo in iterOfDayInfo:
#         # print sortedDayInfo
#         print i
#         # print sortedDayInfo
#         if not graphInit:
#             nodes = sortedDayInfoToDagInfo(sortedDayInfo)
#             if nodes:
#                 dayGraphOut = poset2.DayGraph(nodes)
#                 graphInit=True
#             else:
#                 context.getCounter("REDUCER", "record with 0 days").increment(1)
#         else:
#             nodes = sortedDayInfoToDagInfo(sortedDayInfo)
#             if nodes:
#                 dayGraphToMerge = poset2.DayGraph(nodes)
#                 dayGraphOut.merge(dayGraphToMerge)
#                 context.getCounter("REDUCER", "records merged").increment(1)
#         i+=1
#     if dayGraphOut:
#         # print "dayGraph",dayGraphOut
#         # print 'dayGraphOut._possibleMaxEltIds',dayGraphOut._possibleMaxEltIds
#         # print 'dayGraphOut.maxElts()',dayGraphOut.maxElts()
#         dayGraphOut.addChildTreeWidths()
#         dayGraphOut.addWidthOffsets()
#         try:
#             context.write(partId, str(dayGraphOut))
#         except:
#             print dayGraphOut.nodeDict
#             raise
#         context.getCounter("REDUCER", "partsMerged").increment(1)
#     else:
#         context.getCounter("REDUCER", "PART with 0 days").increment(1)


        


# @orphUtils.localTextInput()
# @healthreportutils.FHRMapper()
# def map(partId,fhrPayload,context):
#     sortedDayInfo = [("bottom",{"date":"0000-00-00","data":"None"})] \
#                     + [(str(hash(date+orphUtils.dictToSortedJsonish(data))),
#                         {"date":date,"data":data})
#                         for date,data in fhrPayload.daily_data()] \
#                     + [("top",{"date":"9999-99-99","data":"None"})]
#     nodes,edges = sortedDayInfoToDagInfo(sortedDayInfo)
#     dayGraphOut = dag.DayDag(nodes,edges)
#     dayGraphOut.addYOffset()
#     dayGraphOut.addSubtreeWidths()
#     context.write(partId,dayGraphOut)

