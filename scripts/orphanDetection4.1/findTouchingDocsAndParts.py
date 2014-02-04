import json
import jydoop
import orphUtils


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True

def num_reduce_tasks():
    return 25


setupjob = orphUtils.hdfsjobByType("JYDOOP")





#partSet is conceptually a set, but will be implemented as a "|" separated set of strings for compatibility reasons


@orphUtils.localTextInput(evalTup=True)
def map(docId, partId_orTieBreakInfo, context):
    # partId_orTieBreakInfo will be either:
    #   (1) a docId string prefixed with a "p", or
    #   (2) a tuple of tieBreakInfo: (date,numAppSessionsPreviousOnThisPingDate, currentSessionTime)
    # we drop all the tieBreakInfos
    context.getCounter("MAPPER", "(docId,partId_orTieBreakInfo) in").increment(1)
    if partId_orTieBreakInfo[0]=="p":
        # if we have an input part id, pass it to a tuple for down-stream set operations
        context.write(docId,tuple([partId_orTieBreakInfo]))
        context.getCounter("MAPPER", "(docId,partSet) passed to reducer").increment(1)
        return
    elif type(partId_orTieBreakInfo)==type(tuple()):
        context.getCounter("MAPPER", "tieBreakInfo dropped").increment(1)
        return
    else:
        print "foo",partId_orTieBreakInfo,type(partId_orTieBreakInfo)
        raise TypeError


def combine(docId, partIdIter, context):
    # partIdIter should always reach the combiner as an iter of tuples like:
    #     [("pid_1"),("pid_2"),...].
    # need to union these tuples and emit ("pid_1","pid_2",...)
    partSetOut = set()
    for partId in partIdIter:
        partSetOut |= set(partId)
    context.write(docId,tuple(partSetOut))



def reduce(docId, partSetIter, context):
    # partIdIter should always reach the combiner as an iter of tuples like:
    #     [("pid_1,1","pid_1,2",...),("pid_2"),("pid_3,1",...)...].

    linkedParts = set()
    for partSet in partSetIter:
        linkedParts |= set(partSet)

    lowPart = min(linkedParts)

    context.write(lowPart,docId)
    context.getCounter("REDUCER", "(lowPart,docId) out").increment(1)

    if len(linkedParts)==1:
        #if there is only one part linked to this docId, there are no overlaps here, so no part-to-part touches are emitted
        context.getCounter("REDUCER", "OVERLAPPING_PARTS").increment(0)
    else:
        #otherwise, return all the overlaps
        context.write(lowPart,tuple(linkedParts))
        context.getCounter("REDUCER", "OVERLAPPING_PARTS").increment(1)




