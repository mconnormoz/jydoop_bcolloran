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


# setupjob = jydoop.setupjob


def setupjob(job, args):
    """
    Set up a job to run on a list of paths.  Jobs expect at least one path,
    but you may specify as many as you like.
    """

    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat as MyInputFormat

    if len(args) < 1:
        raise Exception("Usage: path [ path2 ] [ path3 ] [ ... ]")

    job.setInputFormatClass(MyInputFormat)
    FileInputFormat.setInputPaths(job, ",".join(args));
    """Indicate to HadoopDriver which Mapper we want to use."""
    job.getConfiguration().set("org.mozilla.jydoop.mappertype", "JYDOOP")

def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)



def map(key,val,context):
    context.write(str(key),str(val))