import json
import jydoop
import healthreportutils
import random
import orphUtils



######## to OUTPUT TO HDFS
# def skip_local_output():
#     return True

def num_reduce_tasks():
    return 25


setupjob = orphUtils.hdfsjobByType("JYDOOP")

output = orphUtils.outputKeysOnly



@orphUtils.localTextInput(evalTup=True)
def map(partId, docId_tieBreakInfo, context):
    # this must be:
    #   k: "partId"
    #   v: ("docId",(thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime))
    context.getCounter("MAPPER", "input: docIds with tieBreakInfo (and partId)").increment(1)
    context.write(partId,docId_tieBreakInfo)


def reduce(partId, iter_docId_tieBreakInfo, context):
    context.getCounter("REDUCER", "number of parts").increment(1)
    maxRecordDocIdList = None
    maxRecordTieBreakInfo = ("0000-00-00",0,0)
    # saveIter=[]
    for docId, tieBreakInfo in iter_docId_tieBreakInfo:
        # saveIter+=[(docId, tieBreakInfo)]
        if tieBreakInfo>maxRecordTieBreakInfo:
            #if this is the maximal record, update the maxRecordTieBreakInfo and reset the maxRecordDocIdList
            # print partId,docId,tieBreakInfo
            maxRecordDocIdList=[docId]
            maxRecordTieBreakInfo=tieBreakInfo
        elif tieBreakInfo==maxRecordTieBreakInfo:
            #if this record is tied for maximal record, add it to the list of record tups that tie for max
            maxRecordDocIdList+=[docId]


    #not sure why this was happening, but for some record(s) maxRecordDocIdList was not being set, which means that for all records with the given fingerprint, it must be that:
    # (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime) < ("0000-00-00",0,0)
    # this should only be possible if there is a bad thisPingDate, in which case we will discard the fingerprint
    if maxRecordDocIdList:
        if len(maxRecordDocIdList)==1:
            docIdOut = maxRecordDocIdList[0]

            context.getCounter("REDUCER", "unique naive head records").increment(1)
            context.getCounter("REDUCER", "FINAL HEAD RECORD docIds OUT").increment(1)

        if len(maxRecordDocIdList)>1:
            docIdOut = random.choice(maxRecordDocIdList)

            context.getCounter("REDUCER", "parts with records tied for naive head").increment(1)
            context.getCounter("REDUCER", "records tied for naive head").increment(len(maxRecordDocIdList))
            context.getCounter("REDUCER", "FINAL HEAD RECORD docIds OUT").increment(1)

        context.write(docIdOut, 1)










