import jydoop
import orphUtils


'''
in following commands, UPDATE DATES

make ARGS="scripts/hdfsTools/dumpHdfsKeyValsToStrings.py ./outData/PATH_ON_PEACH_TO_DUMP_TO ./PATH_ON_HDFS_TO_DUMP" hadoop

make ARGS="scripts/hdfsTools/dumpHdfsKeyValsToStrings.py outData/orphanDetection4/linkedRecordSample_2014-02-03.fhrRaw /user/bcolloran/data/linkedRecordSample_2014-02-03" hadoop
'''

output = orphUtils.outputTabSep
setupjob = orphUtils.hdfsjobByType("TEXT")



def map(key,val,context):
    context.write(str(key),str(val))