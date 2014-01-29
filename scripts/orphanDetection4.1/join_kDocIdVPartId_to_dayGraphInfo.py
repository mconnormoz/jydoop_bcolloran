import json
import jydoop
import orphUtils


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True

def hdfs_text_output():
    return True

def num_reduce_tasks():
    return 23


setupjob = orphUtils.hdfsjobByType("JYDOOP")


'''
This script will be passed in pairs like EITHER:
(A)
k: docId
v: partId_orTieBreakInfo, where
    # partId_orDayGraphIter will be either:
    #   (1) a partId (a docId string prefixed with a "p", or)
    #   (2) a dayGraphInfo tuple
    # we drop all partIds, which are obselete from the first pass

(B)
k: partId
v: docId

the mapper will pass out pairs like:
(A)
k: docId
v: dayGraphInfo
(b)
k: docId
v: partId

'''


@orphUtils.localTextInput(evalTup=True)
def map(partId_orDocId, docId_orPartIdOrDayGraphInfo, context):
    context.getCounter("MAPPER", "input (k,v) pairs").increment(1)
    if partId_orDocId[0]=="p":
        #handle case of partId keys first:
        context.getCounter("MAPPER", "(docId,partId) out").increment(1)
        context.write(docId_orPartIdOrDayGraphInfo,partId_orDocId)
        
    elif partId_orDocId[0].lower() in list("0123456789abcdef"):
        # in this case we have a docId key. if the corresponding val is a partId, it can be dropped
        if docId_orPartIdOrDayGraphInfo[0]=="p":
            #skip the partId
            context.getCounter("MAPPER", "obselete partId skipped").increment(1)
            return
        elif type(docId_orPartIdOrDayGraphInfo)==type(()):
            #this should be dayGraphInfo, pass it
            context.getCounter("MAPPER", "(docId,dayGraphInfo) out").increment(1)
            context.write(partId_orDocId,docId_orPartIdOrDayGraphInfo)
        else:
            print "bad input docId_orPartIdOrDayGraphInfo:",docId_orPartIdOrDayGraphInfo
            raise ValueError()
    else:
        print "bad input partId_orDocId:",docId_orPartSet
        raise ValueError()





def reduce(docId,iterOfPartId_orDayGraphInfo, context):
    # for each docId, the iter should have exactly two elements:
    #   partId
    #   dayGraphInfo tuple
    partFlag=False
    dayGraphFlag=False
    numItems=0
    for item in iterOfPartId_orDayGraphInfo:
        numItems+=1
        if item[0]=="p":
            partId=item
            partFlag=True
        elif type(item)==type(()):
            dayGraphInfo=item
            dayGraphFlag=True
        else:
            print "bad reducer iter contents:",item
            raise ValueError()

    if numItems==2:
        context.getCounter("REDUCER", "(partId,dayGraphInfo) out").increment(1)
        context.write(partId,dayGraphInfo)
    else:
        context.getCounter("REDUCER", "partFlag:"+str(partFlag)+" dayGraphFlag"+str(dayGraphFlag)).increment(1)
        # print "iter in reducer does not have 2 elts. each docId key should correspond to exactly one partId and one dayGraphInfo"
        # print docId, list(iterOfPartId_orDayGraphInfo)
        # raise ValueError()



