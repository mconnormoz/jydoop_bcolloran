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
    return 25


setupjob = orphUtils.hdfsjobByType("JYDOOP")



#partIds will be passed in as plain strings

#partSet is conceptually a set, but must be implemented as a tuple for jydoop to not break
#docIds will be passed in as plain strings

@orphUtils.localTextInput(evalTup=True)
def map(partId, docId_orPartSet, context):
    # docId_orPartSet will be like either
    #     "c232ecdc-a56a-41be-a78a-aadf748f9408"
    # OR
    #     ('p_1','p_2',...)
    # want to pass out ONLY TUPLES

    context.getCounter("MAPPER", "(partId,docId_orPartSet) in").increment(1)
    
    if type(docId_orPartSet)==type("a string"):
        # wrap docIds in tuples
        context.write(partId,tuple([docId_orPartSet]))
        context.getCounter("MAPPER", "(partId,docIdSet) out").increment(1)
    elif type(docId_orPartSet)==type(tuple()):
        # pass partSets unchanged.
        context.write(partId,docId_orPartSet)
        context.getCounter("MAPPER", "(partId,partSet) out").increment(1)
    else:
        print "bad docId_orPartSet:",docId_orPartSet
        raise ValueError()




def combine(partId,iterOfDocIdSet_orPartSet, context):
    # iterOfDocIdSet_orPartSet will be like 
    #     [("docId_1"),('p_1','p_2',...),('p_n+1','p_n+2',...),("docId_2"),...]
    # want to emit
    #     [("docId_1",...),('p_1','p_2',...),...]
    partSetOut = set()
    docSetOut = set()
    for docIdSet_orPartSet in iterOfDocIdSet_orPartSet:
        if docIdSet_orPartSet[0][0].lower() in list("0123456789abcdef"):
            docSetOut |= set(docIdSet_orPartSet)
        elif docIdSet_orPartSet[0][0]=="p":
            partSetOut |= set(docIdSet_orPartSet)
        else:
            print "bad docId_orPartSet:",docIdSet_orPartSet
            raise ValueError()

    if len(docSetOut)>0:
        context.write(partId,tuple(docSetOut))
    if len(partSetOut)>0:
        context.write(partId,tuple(partSetOut))




def reduce(partId,iterOfDocIdSet_orPartSet, context):
    # iterOfDocIdSet_orPartSet will be like 
    #     [("docId_1"),('p_1','p_2',...),('p_n+1','p_n+2',...),("docId_2"),...]
    # want to emit
    #     [("docId_1",...),('p_1','p_2',...),...]
    context.getCounter("REDUCER", "distinct parts at this iter").increment(1)
    partSet = set([partId])
    docSet = set()

    for docIdSet_orPartSet in iterOfDocIdSet_orPartSet:
        if docIdSet_orPartSet[0][0].lower() in list("0123456789abcdef"):
            docSet |= set(docIdSet_orPartSet)
        elif docIdSet_orPartSet[0][0]=="p":
            partSet |= set(docIdSet_orPartSet)
        else:
            print "bad docId_orPartSet:",docIdSet_orPartSet
            raise ValueError()

    # for docIdSet_orPartSet in iterOfDocIdSet_orPartSet:
    #     if type(docIdSet_orPartSet)==type("a string"):
    #         docSet |= set([docIdSet_orPartSet])
    #     elif type(docIdSet_orPartSet)==type(tuple()) and :
    #         partSet|= set(docIdSet_orPartSet)
    #     else:
    #         raise ValueError()

    lowPart=min(partSet)

    for docId in docSet:
        context.write(docId,lowPart)
        context.getCounter("REDUCER", "docIds out").increment(1)



