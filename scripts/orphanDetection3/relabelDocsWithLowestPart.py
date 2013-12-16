import json
import jydoop
import orphUtils


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True


setupjob = orphUtils.hdfsjobByType("JYDOOP")





#partSet is conceptually a set, but must be implemented as a tuple for jydoop to not break



@orphUtils.localTextInput(evalVal=True)
def map(partId, docOrPartSet, context):
    context.getCounter("MAPPER", "(partId,docOrPartSet) in").increment(1)
    context.write(partId,docOrPartSet)




def combine(partId,iterOfDocOrPartSet, context):
    partSetOut = set()
    docSetOut = set()
    for docOrPartSet in iterOfDocOrPartSet:
        if docOrPartSet[0]=="d":
            docSetOut |= set(docOrPartSet[1:])
        elif docOrPartSet[0]=="p":
            partSetOut |= set(docOrPartSet[1:])
        else:
            print "docOrPartSet[0] must be 'p' or 'd'"
            print "docOrPartSet",docOrPartSet
            print "docOrPartSet[0]", docOrPartSet[0]
            raise ValueError()

    if len(docSetOut)>0:
        context.write(partId,tuple("d")+tuple(docSetOut))
    if len(partSetOut)>0:
        context.write(partId,tuple("p")+tuple(partSetOut))




def reduce(partId,iterOfDocOrPartSet, context):
    context.getCounter("REDUCER", "distinct parts at this iter").increment(1)
    partSet = set([partId])
    docSet = set()
    for docOrPartSet in iterOfDocOrPartSet:
        if docOrPartSet[0]=="d":
            docSet |= set(docOrPartSet[1:])
        elif docOrPartSet[0]=="p":
            partSet |= set(docOrPartSet[1:])
        else:
            print "docOrPartSet[0] must be 'p' or 'd'"
            print "docOrPartSet",docOrPartSet
            print "docOrPartSet[0]", docOrPartSet[0]
            raise ValueError()

    lowPart=min(partSet)

    for docId in docSet:
        context.write(docId,lowPart)
        context.getCounter("REDUCER", "docIds out").increment(1)



