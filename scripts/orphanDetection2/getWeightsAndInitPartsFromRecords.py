import json
# import healthreportutils
import jydoop

'''
in following commands, UPDATE DATES

----to run against full HBASE, output to HDFS
jydoopRemote peach scripts/getWeightsAndInitPartsFromRecords.py outData/findRecordsSharingDatePrint_test


----to run against HDFS sample, output to HDFS; on peach:
make ARGS="scripts/orphanDetection2/getWeightsAndInitPartsFromRecords.py ./outData/orphIterTest3/kEdge_vPart_0 ./data/samples/fhr/v2/withOrphans/2013-11-05" hadoop

'''


def output(path, results):
    # just dump tab separated key/vals
    firstLine = True
    with open(path, 'w') as f:
        for k, v in results:
            if firstLine:
                f.write(str(k)+"\t"+str(v))
                firstLine=False
            else:
                f.write("\n"+str(k)+"\t"+str(v))
    # print context.localCounterDict

# context.localCounterDict="\n\nFOOOOOOOOOOOOOOOOOOOOOOOOOOO\n\n"

def counterLocal(context,counterGroup,countername,value):
    if jydoop.isJython():
        context.getCounter(counterGroup, countername).increment(value)
    else:
        pass


######## to OUTPUT TO HDFS
def skip_local_output():
    return True


setupjob = jydoop.setupjob

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
    job.getConfiguration().set("mapred.job.queue.name","research")



def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the line key. split
    if jydoop.isJython():
        return mapper
    else:
        def localMapper(lineKey,inputLine,context):
            keyValList = inputLine.split("\t")
            return mapper(keyValList[0],keyValList[1],context)
        return localMapper


# need to use this since python dicts don't guarantee order, and since json.dumps with sorting flag is broken in jydoop
def dictToSortedTupList(objIn):
    if isinstance(objIn,dict):
        return [(key,dictToSortedTupList(val)) for key,val in sorted(objIn.items(),key=lambda item:item[0])]
    else:
        return objIn


def jaccard(a, b):
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))













@localTextInput
def map(fhrDocId, rawJsonIn, context):
    context.getCounter("MAPPER", "docs_in").increment(1)

    try:
        payload = json.loads(rawJsonIn)
    except:
        return

    try:
        payloadVersion = payload["version"]
    except: #was getting errors finding packets without a version
        return

    if payloadVersion != 2:
        return

    #NOTE: we drop any packet without data.days entries. these cannot be fingerprinted/linked.
    try:
        dataDays = payload["data"]["days"].keys()
    except:
        return

    #NOTE: we will use profile creation date to add further refinement to date colisions, but it is not required.
    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except:
        profileCreation = "no_profileCreation"


    datePrints = tuple([ str(profileCreation)+"_"+date+"_"+str(hash(str(dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() ])
    
    
    for d in datePrints:
        # print (d,profileCreation)
        # print (fhrDocId,datePrints)
        context.write(d,(fhrDocId,datePrints))











def reduce(datePrint, valIter, context):
    context.getCounter("REDUCER", "datePrint_in").increment(1)
    # a given datePrint can only be associated with a given record ONCE, because a date print cannot appear twice in the same record, so it will never be possible for identical (datePrint,recordInfo) pairs to be emitted in the map phase

    # valIter contains (fhrDocId,datePrints); sort these by fhrDocId
    recordInfoList = sorted(valIter,key=lambda tup:tup[0])

    # note: if this datePrint only appears in one record, no edge will be emitted
    for i in range(len(recordInfoList)):
        for j in range(i+1,len(recordInfoList)):
            daysA = set(recordInfoList[i][1])
            daysB = set(recordInfoList[j][1])
            daysBoth = daysA.intersection(daysB)
            daysEither = daysA.union(daysB)
            weightInfo = (float(len(daysBoth))/float(len(daysEither)),len(daysA),len(daysB),len(daysBoth),len(daysEither))

            context.getCounter("REDUCER", "edges_out").increment(1)

            context.write(
                (recordInfoList[i][0],recordInfoList[j][0],weightInfo),
                ("PART",recordInfoList[i][0])
                )




