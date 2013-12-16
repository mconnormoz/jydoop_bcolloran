import jydoop
import json
'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/docIdsInParts.py ./outData/finalDocIdsInParts ./outData/partsOverlap" hadoop

'''

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
    #just ignore the lineKey.
    if jydoop.isJython():
        return mapper
    else:
        def localMapper(lineKey,inputLine,context):
            keyValList = inputLine.split("\t")
            key = keyValList[0]

            if keyValList[1][0:7]=="('PART'":
                val = eval(keyValList[1])
            else:
                val = keyValList[1]

            return mapper(key,val,context)
        return localMapper

class edgeTupError(Exception):
    def __init__(self, part, listOfEdges, iterOfEdges):
        self.part = part
        self.listOfEdges = listOfEdges
        self.iterOfEdges =iterOfEdges
    def __str__(self):
        return repr((self.part, self.listOfEdges,self.iterOfEdges))











@localTextInput
def map(docId,partOrJson,context):
    if type(partOrJson)==tuple: #the part for these docs
        # emit ONLY the partId for the doc, which is partOrJson[1]
        context.write(docId,partOrJson[1])
        context.getCounter("MAPPER", "input parts (docId,part) pairs").increment(1)


    else: #an FHR record; extract the tie breaker info
        context.getCounter("MAPPER", "input (docId,rawjson) pairs").increment(1)
        try:
            payload = json.loads(partOrJson)
        except KeyError:
            context.getCounter("MAP ERROR", "record failed to parse").increment(1)
            context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
            return

        try: #was getting errors finding packets without a version field, so had to wrap this test in a try block
            if not (payload["version"]==2):
                context.getCounter("MAPPER", "record not v2").increment(1)
                return
        except KeyError:
            context.getCounter("MAP ERROR", "no version").increment(1)
            context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
            return

        try:
            thisPingDate = payload["thisPingDate"]
        except KeyError:
            context.getCounter("MAP ERROR", "no thisPingDate").increment(1)
            context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
            return

        try:
            numAppSessionsPreviousOnThisPingDate=len(payload["data"]["days"][thisPingDate]['org.mozilla.appSessions.previous']["main"])
        except KeyError:
            context.getCounter("MAP WARNING", "no ['...appSessions.previous']['main'] on thisPingDate").increment(1)
            numAppSessionsPreviousOnThisPingDate = 0
        except TypeError:
            #was getting "TypeError: 'float' object is unsubscriptable" errors in the above. this should not happen, and must indicate a bad packet, which we will discard
            context.getCounter("MAP ERROR", "float instead of obj in ['...appSessions.previous']['main'] on thisPingDate  ").increment(1)
            context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
            return

        try:
            currentSessionTime=payload["data"]["last"]['org.mozilla.appSessions.current']["totalTime"]
        except KeyError:
            currentSessionTime = 0
            context.getCounter("MAP WARNING", "no currentSessionTime").increment(1)

        context.write(docId,
            (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime) )




def reduce(docId, iterOfPartOrTieBreakInfo, context):
    #since each doc id is unique, it should be associated with ONLY 1 partId, and ONLY 1 tuple of tieBreakInfo; so iterOfPartOrTieBreakInfo should only have 2 elts

    numTieBreakPackets=0
    numPartId = 0
    for partOrJson in iterOfPartOrTieBreakInfo:
        if type(partOrJson)==tuple: #the tie break info for this docs
            tieBreakInfo = partOrJson
            numTieBreakPackets+=1
        else: #the partId, a str
            partId = partOrJson
            numPartId+=1

    if numTieBreakPackets==1 and numPartId==1:
        context.write(partId,(docId,tieBreakInfo))
        context.getCounter("REDUCER", "OK (partId,(docId,tieBreakInfo)) k/v pair").increment(1)
        return
    elif numTieBreakPackets==0 and numPartId==1:
        #in this case, if there is no tie break info, then the record must  be one that is not linked to any other record. Emit the docId assigned to its own partId, and fill in dummy tieBreakInfo, since there are no ties to break.
        context.write(docId,(docId,("2000-01-01",0,0)))
        context.getCounter("REDUCER", "docId not linked to any other doc").increment(1)
        return
    elif numTieBreakPackets==1 and numPartId==0:
        context.getCounter("RED ERROR", "docId with tieBreakInfo, but no part").increment(1)
        return
    elif numTieBreakPackets==0 and numPartId==0:
        context.getCounter("RED ERROR", "WTF docId with no part and no tieBreakInfo?").increment(1)
        return
    elif numTieBreakPackets>1 or numPartId>1:
        context.getCounter("RED ERROR", "docId with too many tieBreakInfos or partIds").increment(1)
        return






