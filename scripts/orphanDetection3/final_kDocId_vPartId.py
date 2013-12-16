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
def map(partId, docIdTuple, context):
    context.getCounter("MAPPER", "(partId,docId) in").increment(1)
    context.write(docIdTuple[1],("PART",partId))







