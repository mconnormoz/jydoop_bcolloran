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



@orphUtils.localTextInput(evalTup=True)
def map(partId_orDocId, docId_orPartIdOrRawJson, context):
    context.getCounter("MAPPER", "input (k,v) pairs").increment(1)
    if partId_orDocId[0]=="p":
        #handle case of partId keys first:
        context.getCounter("MAPPER", "(docId,partId) out").increment(1)
        context.write(docId_orPartIdOrRawJson,partId_orDocId)
        
    elif partId_orDocId[0].lower() in list("0123456789abcdef"):
        # in this case we have a docId key. if the corresponding val is a partId, it can be dropped
        if docId_orPartIdOrRawJson[0]=="p":
            #skip the partId
            context.getCounter("MAPPER", "obselete partId skipped").increment(1)
            return
        elif docId_orPartIdOrRawJson[0]=="{":
            #this should be rawJson, pass it
            context.getCounter("MAPPER", "(docId,rawJson) out").increment(1)
            context.write(partId_orDocId,docId_orPartIdOrRawJson)
        else:
            print "bad input docId_orPartIdOrRawJson:",docId_orPartIdOrRawJson
            raise ValueError()
    else:
        print "bad input partId_orDocId:",docId_orPartSet
        raise ValueError()





def reduce(docId,iterOfPartId_orRawJson, context):
    # for each docId, the iter should have exactly two elements:
    #   partId
    #   fhrJson string
    numItems=0
    for item in iterOfPartId_orRawJson:
        numItems+=1
        if item[0]=="p":
            partId=item
        elif item[0]=="{":
            rawJson=item
        else:
            print "bad reducer iter contents:",item
            raise ValueError()

    if numItems==2:
        context.getCounter("REDUCER", "(partId,rawJson)  out").increment(1)
        context.write(partId,rawJson)
    else:
        print "iter in reducer does not have 2 elts. each docId key should correspond to exactly one partId and one rawJson"
        print docId, list(iterOfPartId_orRawJson)
        raise ValueError()



