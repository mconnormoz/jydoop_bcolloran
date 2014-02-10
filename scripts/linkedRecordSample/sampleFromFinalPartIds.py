import json
import jydoop
import orphUtils
import random


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True

def hdfs_text_output():
    return True

def num_reduce_tasks():
    return 23


setupjob = orphUtils.hdfsjobByType("JYDOOP")


"""
this script will process a file of:
kPart_vObjTouchingPart_${finalIter}
which must be in final form, which means that it should be onl (docId,partId) pairs.

it will return a sample of
(docId,partId) pairs
"""


@orphUtils.localTextInput(evalTup=True)
def map(partId, docId, context):
    context.getCounter("MAPPER", "input (k,v) pairs").increment(1)
    if partId[0]!="p":
        print "key must be a partId:",(partId, docId)
        raise ValueError()

    if docId[0].lower() not in list("0123456789abcdef"):
        print "val must be a docId:",(partId, docId)
        raise ValueError()

    context.getCounter("MAPPER", "(docId,partId) out").increment(1)
    context.write(partId,docId)


def reduce(partId,iterOfDocIds,context):
    context.getCounter("REDUCER", "parts in").increment(1)
    listOfDocIds=list(iterOfDocIds)
    context.getCounter("REDUCER", "docs in").increment(len(listOfDocIds))

    if 1<len(listOfDocIds)<101 and random.random()<.001:
        context.getCounter("REDUCER", "sampled parts out").increment(1)
        for docId in listOfDocIds:
            context.getCounter("REDUCER", "sampled docs out").increment(1)
            context.write(docId,partId)



