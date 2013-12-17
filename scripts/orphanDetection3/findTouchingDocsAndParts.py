import json
import jydoop
import orphUtils


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True

def num_reduce_tasks():
    return 23


setupjob = orphUtils.hdfsjobByType("JYDOOP")





#partSet is conceptually a set, but must be implemented as a tuple for jydoop to not break



@orphUtils.localTextInput()
def map(docId, partSet, context):
    context.getCounter("MAPPER", "(docId,partNum) in").increment(1)
    if type(partSet)==type("sdfg"):
        #initially, the partSet may be a single partId string
        context.write(docId,tuple([partSet]))

    elif type(partSet) == type(tuple()):
        context.write(docId,partSet)
    else:
        raise


def combine(docId, partSetIter, context):
    partSetOut = set()
    for partSet in partSetIter:
        partSetOut |= set(partSet)
    context.write(docId,tuple(partSetOut))






def reduce(docId, partSetIter, context):

    linkedParts = set()
    for partSet in partSetIter:
        linkedParts |= set(partSet)

    lowPart = min(linkedParts)

    context.write(lowPart,("d",docId))
    context.getCounter("REDUCER", "(lowPart,docId) out").increment(1)

    if len(linkedParts)==1:
        #if there is only one part linked to this docId, there are no overlaps here, so no part-to-part touches are emitted
        context.getCounter("REDUCER", "OVERLAPPING_PARTS").increment(0)
    else:
        #otherwise, return all the overlaps
        context.write(lowPart,tuple("p")+tuple(linkedParts))
        context.getCounter("REDUCER", "OVERLAPPING_PARTS").increment(1)




