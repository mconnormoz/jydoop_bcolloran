import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/dumpHdfsKeyValsToStrings.py ./outData/PATH_ON_PEACH_TO_DUMP_TO ./PATH_ON_HDFS_TO_DUMP" hadoop

/user/bcolloran/outData/orphIterTest/edgeWeightsAndInitParts
/user/bcolloran/outData/orphIterTest/partsOverlap2
/user/bcolloran/outData/orphIterTest/partsOverlap3
/user/bcolloran/outData/orphIterTest/relabeledEdges1


make ARGS="scripts/dumpHdfsKeyValsToStrings.py ./outData/edgeWeightsAndInitParts ./outData/orphIterTest/edgeWeightsAndInitParts" hadoop

make ARGS="scripts/dumpHdfsKeyValsToStrings.py ./outData/partsOverlap2 ./outData/orphIterTest/partsOverlap2" hadoop

'''


setupjob = jydoop.setupjob


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)



def map(key,val,context):
    context.write(str(key),str(val))