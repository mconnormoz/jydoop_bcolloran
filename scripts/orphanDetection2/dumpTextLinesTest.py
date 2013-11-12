import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/dumpFhrDocsByDocId.py ./outData/PATH_ON_PEACH_TO_DUMP_TO ./PATH_ON_HDFS_TO_DUMP" hadoop

'''


setupjob = jydoop.setupjob


def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the line key. split
    def localMapper(lineKey,inputLine,context):
        keyValList = inputLine.split("\t")
        return mapper(eval(keyValList[0]),eval(keyValList[1]),context)
    return localMapper


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)


@localTextInput
def map(key,val,context):
    context.write(key,val)

