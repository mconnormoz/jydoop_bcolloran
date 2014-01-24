import jydoop
import healthreportutils
import json
import datetime

import orphUtils
import dag

output = orphUtils.outputTabSep


setupjob = orphUtils.hdfsjobByType("HDFS")

def output(path, results):
    # just dump tab separated key/vals
    partNum=0
    for k, vList in results:
        partNum+=1
        with open(path+"_numRecs"+str(len([v for v in vList if v[0:9]=='{"version']))+("_graph" if [v for v in vList if v[0:7]=='{"nodes'] else "")+"_partNum"+str(partNum), 'w') as f:
            firstLine = True
            for v in vList:
                if firstLine:
                    # print repr(v)
                    f.write(str(v).strip())
                    firstLine=False
                else:
                    f.write("\n"+str(v).strip())




def sortedDayInfoToDagInfo(sortedDayInfo):
    nodes = {label:info for label,info in sortedDayInfo}
            #addLinks
    edges = {(sortedDayInfo[i][0],sortedDayInfo[i+1][0]):1 for i in range(len(sortedDayInfo)-1)}
    return (nodes,edges)


@orphUtils.localTextInput()
@healthreportutils.FHRMapper()
def map(partId,fhrPayload,context):
    sortedDayInfo = [("bottom",{"day":None,"data":None})] \
                    + [(hash(orphUtils.dictToSortedJsonish(data)),
                        {"day":day,"data":data})
                        for day,data in fhrPayload.daily_data()] \
                    + [("top",{"day":None,"data":None})]
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
            dayGraphOut = dag.dayDag(nodes,edges)
            firstIter=False
            context.write(partId,dayGraphOut)
        else:
            nodes,edges = sortedDayInfoToDagInfo(sortedDayInfo)
            dayGraph = dag.dayDag(nodes,edges)
            dayGraphOut.merge(dayGraph)
            context.write(partId,dayGraph)
    context.write(partId,dayGraphOut)
    context.getCounter("REDUCER", "partsMerged").increment(1)
        







