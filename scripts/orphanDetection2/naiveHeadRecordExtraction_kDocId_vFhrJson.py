import json
import jydoop
import healthreportutils
import random



######## to OUTPUT TO HDFS
def skip_local_output():
    return True


setupjob = jydoop.setupjob


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


def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the line key. split
    if jydoop.isJython():
        return mapper
    else:
        def localMapper(lineKey,inputLine,context):
            keyValList = inputLine.split("\t")
            key = keyValList[0]

            val = keyValList[1].rstrip()

            return mapper(key,val,context)
        return localMapper




@localTextInput
def map(fhrDocId, rawJsonIn, context):

    try:
        payload = json.loads(rawJsonIn)
    except KeyError:
        context.getCounter("ERROR", "record failed to parse").increment(1)
        return

    try: #was getting errors finding packets without a version field, so had to wrap this test in a try block
        if not (payload["version"]==2):
            context.getCounter("MAPPER", "record not v2").increment(1)
    except KeyError:
        context.getCounter("ERROR", "no version").increment(1)
        return

    try:
        thisPingDate = payload["thisPingDate"]
    except KeyError:
        context.getCounter("ERROR", "no thisPingDate").increment(1)
        return

    try:
        numAppSessionsPreviousOnThisPingDate=len(payload["data"]["days"][thisPingDate]['org.mozilla.appSessions.previous']["main"])
    except KeyError:
        context.getCounter("ERROR", "no ['...appSessions.previous']['main'] on thisPingDate").increment(1)
        numAppSessionsPreviousOnThisPingDate = 0
    except TypeError:
        #was getting "TypeError: 'float' object is unsubscriptable" errors in the above. this should not happen, and must indicate a bad packet, which we will discard
        context.getCounter("ERROR", "float instead of obj in ['...appSessions.previous']['main'] on thisPingDate").increment(1)

    try:
        currentSessionTime=payload["data"]["last"]['org.mozilla.appSessions.current']["totalTime"]
    except KeyError:
        currentSessionTime = 0
        context.getCounter("ERROR", "no currentSessionTime").increment(1)


    context.write(partId,
        (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime, fhrDocId) )







def reduce(k, vIter, cx):

    maxRecordTupSortKey = None #initialize as a None
    maxRecordTupList = None
    maxDate="0000-00-00"
    maxSessionsOnMaxDate=0
    maxSessTimeOnMaxDate=0

    maxRecordTupSortKey = ("0000-00-00",0,0)
    for valTup in vIter:
        if valTup[0:3]>maxRecordTupSortKey:
            #if this is the maximal record, update the maxRecordTupSortKey and reset the maxRecordTupList
            maxRecordTupList=[valTup]
            maxRecordTupSortKey=valTup[0:3]
        elif valTup==maxRecordTupSortKey:
            #if this record is tied for maximal record, add it to the list of record tups that tie for max
            maxRecordTupList+=[valTup]


    #not sure why this was happening, but for some record(s) maxRecordTupList was not being set, which means that for all records with the given fingerprint, it must be that:
    # (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime) < ("0000-00-00",0,0)
    # this should only be possible if there is a bad thisPingDate, in which case we will discard the fingerprint
    if maxRecordTupList:
        if len(maxRecordTupList)==1:
            maxRecordTupOut = maxRecordTupList[0]

        if len(maxRecordTupList)>1:
            print [tup[0:3] for tup in maxRecordTupList],len(maxRecordTupList)
            maxRecordTupOut = random.choice(maxRecordTupList)
            #cx.write( "NON_UNIQUE_HEAD_RECORD" ,maxRecordTup)

        cx.write( maxRecordTupOut[3] , 1)










