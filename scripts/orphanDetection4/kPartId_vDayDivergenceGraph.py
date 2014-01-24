import jydoop
import healthreportutils
import json
import datetime

import orphUtils
import dag

output = orphUtils.outputTabSep


setupjob = orphUtils.hdfsjobByType("HDFS")


def sortedDayInfoToDagInfo(sortedDayInfo):
    maxDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    nodes = [dag.DayNode(id=label,date=info["date"],maxDateInThread=maxDateInThread,data=info["data"]) for label,info in sortedDayInfo]

    edges = [dag.DayEdge(id=str(nodes[i].id)+"_"+str(nodes[i+1].id),
                            source=nodes[i],
                            target=nodes[i+1],
                            count=1) for i in range(len(nodes)-1)]

    # print "nodes:",nodes
    # print "edges:",edges
    return (nodes,edges)


@orphUtils.localTextInput()
@healthreportutils.FHRMapper()
def map(partId,fhrPayload,context):
    sortedDayInfo =  [("bottom",{"date":"0000-00-00","data":"None"})] \
                    + [(str(hash(date+orphUtils.dictToSortedJsonish(data))),
                        {"date":date,"data":data})
                        for date,data in fhrPayload.daily_data()] \
                    + [("top",{"date":"9999-99-99","data":"None"})]
    context.write(partId,sortedDayInfo)


def reduce(partId, iterOfDayInfo, context):
    firstIter=True
    i=0
    # print i
    for sortedDayInfo in iterOfDayInfo:
        i+=1
        # print i
        if firstIter:
            nodes,edges = sortedDayInfoToDagInfo(sortedDayInfo)
            dayGraphOut = dag.DayDag(nodes,edges)
            firstIter=False
        else:
            nodes,edges = sortedDayInfoToDagInfo(sortedDayInfo)
            dayGraph = dag.DayDag(nodes,edges)
            dayGraphOut.merge(dayGraph)
    dayGraphOut.addYOffset()
    # dayGraphOut.addSubtreeWidths()
    context.write(partId,dayGraphOut)
    context.getCounter("REDUCER", "partsMerged").increment(1)
    print 1

        







