import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/dumpHdfsKeyValsToStrings.py ./outData/PATH_ON_PEACH_TO_DUMP_TO ./PATH_ON_HDFS_TO_DUMP" hadoop

'''


setupjob = jydoop.setupjob


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)



def map(key,val,context):
    context.write(str(key),str(val))