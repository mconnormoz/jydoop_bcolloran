import json
# import healthreportutils
import jydoop

'''
in following commands, UPDATE DATES

----to run against full HBASE, output to HDFS
jydoopRemote peach scripts/findRecordsSharingDatePrint.py outData/findRecordsSharingDatePrint_test


----to run against full HDFS sample, output to HDFS; on peach:
make ARGS="scripts/orphanDetection2/findRecordsSharingDatePrint.py ./outData/recordsSharingDatePrint /user/bcolloran/outData/v2Packets_sampleWithLinkedOrphans_2013-11-05_v2" hadoop


'''


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)


######## to OUTPUT TO HDFS from RAW HBASE
def skip_local_output():
    return True


setupjob = jydoop.setupjob




# need to use this since python dicts don't guarantee order, and since json.dumps with sorting flag is broken in jydoop
def dictToSortedTupList(objIn):
    if isinstance(objIn,dict):
        return [(key,dictToSortedTupList(val)) for key,val in sorted(objIn.items(),key=lambda item:item[0])]
    else:
        return objIn



def map(fhrDocId, rawJsonIn, context):

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

    try:
        thisPingDate = payload["thisPingDate"]
    except: #was getting errors finding packets without a version
        thisPingDate = "no_pingDate"

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

def jaccard(a, b):
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))

def reduce(datePrint, valIter, context):
    # a given datePrint can only be associated with a given record ONCE, because a date print cannot appear twice in the same record, so it will never be possible for identical (datePrint,recordInfo) pairs to be emitted in the map phase

    # valIter contains (fhrDocId,datePrints); sort these by fhrDocId
    recordInfoList = sorted(valIter,key=lambda tup:tup[0])

    for i in range(len(recordInfoList)):
        for j in range(i+1,len(recordInfoList)):
            daysA = set(recordInfoList[i][1])
            daysB = set(recordInfoList[j][1])
            daysBoth = daysA.intersection(daysB)
            daysEither = daysA.union(daysB)

            context.write(
                (recordInfoList[i][0],recordInfoList[j][0]),
                (float(len(daysBoth))/float(len(daysEither)),len(daysA),len(daysB),len(daysBoth),len(daysEither))
                )




