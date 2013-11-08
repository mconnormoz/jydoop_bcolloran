import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/dumpHdfsKeyValsToStrings.py ./outData/PATH_ON_PEACH_TO_DUMP_TO ./PATH_ON_HDFS_TO_DUMP" hadoop

'''


setupjob = jydoop.setupjob

def map(key,val,context):
    context.write(str(key),str(val))