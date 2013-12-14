import json
import jydoop
import orphUtils


output = orphUtils.outputTabSep


######## to OUTPUT TO HDFS
def skip_local_output():
    return True


setupjob = orphUtils.hdfsjobByType("TEXT")









@orphUtils.localTextInput()
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


    datePrints = [str(profileCreation)+"_"+date+"_"+str(hash(str(orphUtils.dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() ]
    
    
    for d in datePrints:
        context.getCounter("MAPPER", "(datePrint,docId) out").increment(1)
        context.write(d,fhrDocId)











def reduce(datePrint, fhrDocIdIter, context):
    context.getCounter("REDUCER", "datePrint_in").increment(1)
    # a given datePrint can only be associated with a given record ONCE, because a date print cannot appear twice in the same record, so it will never be possible for identical (datePrint,recordInfo) pairs to be emitted in the map phase
    linkedDocIds = list(fhrDocIdIter)
    partNum = min(linkedDocIds)
    for docId in linkedDocIds:
        context.write(docId,partNum)
        context.getCounter("REDUCER", "(docId,partNum) out").increment(1)




