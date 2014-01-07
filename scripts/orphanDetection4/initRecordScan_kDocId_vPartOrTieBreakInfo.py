import json
import jydoop
import orphUtils


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True


setupjob = orphUtils.hdfsjobByType("JYDOOP")




@orphUtils.localTextInput()
def map(docId, rawJsonIn, context):
    context.getCounter("MAPPER", "INPUT (docId,payload)").increment(1)

    try:
        payload = json.loads(rawJsonIn)
    except:
        context.getCounter("MAP ERROR", "record failed to parse").increment(1)
        context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
        return

    try: #was getting errors finding packets without a version field, so had to wrap this test in a try block
        if not (payload["version"]==2):
            context.getCounter("MAPPER", "record not v2").increment(1)
            return
    except:
        context.getCounter("MAP ERROR", "no version").increment(1)
        context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
        return

    #NOTE: we drop any packet without data.days entries. these cannot be fingerprinted/linked.
    try:
        dataDays = payload["data"]["days"].keys()
    except:
        context.getCounter("MAP ERROR", "no dataDays").increment(1)
        context.getCounter("MAP ERROR", "REJECTED RECORDS").increment(1)
        return

    #NOTE: we will use profile creation date to add further refinement to date colisions, but it is not required.
    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except:
        profileCreation = "00000"
        context.getCounter("MAP WARNING", "no profileCreation").increment(1)


    try:
        thisPingDate = payload["thisPingDate"]
    except:
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

    context.getCounter("MAPPER", "(docId,tieBreakInfo) out").increment(1)
    context.write("docId"+docId,
        (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime) )

    datePrints = [str(profileCreation)+"_"+date+"_"+str(hash(str(orphUtils.dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() ]


    for d in datePrints:
        context.getCounter("MAPPER", "(datePrint,docId) out").increment(1)
        context.write(d,docId)

    if not datePrints:
        # NOTE: if there are NO date prints in a record, the record cannot be linked to any others. pass it through with it's own part already determined
        context.getCounter("MAPPER", "unlinkable record with no dates").increment(1)
        context.write("unlinkable_"+docId,"p"+docId)














def reduce(datePrintOrDocId, docIdIter_OrTieBreakIter, context):
    context.getCounter("REDUCER", "datePrintOrDocId keys in to reducer").increment(1)
    if datePrintOrDocId[0:5]=="docId":
        #in this case, we have a (docId,tieBreakIter)
        #only emit tie breaker info for the first docId

        #this list *should* have all identical info, since these docIds should be identical only if the records are exact dupes...
        tieBreakInfoList = list(docIdIter_OrTieBreakIter)

        context.write(datePrintOrDocId[5:], tieBreakInfoList[0])
        context.getCounter("REDUCER", "(docId,tieBreakInfo) out").increment(1)
        if 1<len(tieBreakInfoList)<10:
            context.getCounter("REDUCER", "num docIds shared by "+str(len(tieBreakInfoList))+" records").increment(1)
        elif len(tieBreakInfoList)>=10:
            context.getCounter("REDUCER", "num docIds shared by >=10 records").increment(1)
        return
    elif datePrintOrDocId[0:11]=="unlinkable_":
        # we have a ("unlinkable_"+docId, "p"+docId) pair. pass it. the valIter should only have 1 elt.
        partIdSet = list(set(docIdIter_OrTieBreakIter))
        if len(partIdSet)==1:
            context.write(datePrintOrDocId[11:],partIdSet[0])
        else:
            print "unlinkable docs without days should only have 1 associated part (even if a docId is duplicated)"
            raise ValueError
    else:
        # a given datePrintOrDocId can only be associated with a given record ONCE, because a date print cannot appear twice in the same record, so it will never be possible for identical (datePrintOrDocId,recordInfo) pairs to be emitted in the map phase
        linkedDocIds = list(docIdIter_OrTieBreakIter)
        partNum = min(linkedDocIds)
        for docId in linkedDocIds:
            context.write(docId,"p"+partNum)
            context.getCounter("REDUCER", "(docId,partNum) out").increment(1)




