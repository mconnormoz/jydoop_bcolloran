import orphUtils
import jydoop

output = orphUtils.outputTabSep

######## to OUTPUT TO HDFS
# def skip_local_output():
#     return True


setupjob = orphUtils.hdfsjobByType("JYDOOP")



@orphUtils.localTextInput(evalTup=True)
def map(partId,numRecs_numHeads,context):
    # print type(sortedDayInfo)
    context.write(str(numRecs_numHeads[0])+","+str(numRecs_numHeads[1]),1)


reduce = jydoop.sumreducer







