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


    datePrints = tuple([ profileCreation+"_"+date+"_"+hash(str(dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() ])
    
    recordInfo = (len(dataDays),thisPingDate)
    
    for d in datePrints:
        # print (d,profileCreation)
        # print (fhrDocId,datePrints)
        context.write(d,(fhrDocId,datePrints))


def reduce(datePrint, vIter, context):
    recordInfoList = sorted(list(set(vIter)),key=lambda tup:tup[0])
    for i in range(len(recordInfoList)):
        for j in range(i+1,len(recordInfoList)):
            context.write(
                (recordInfoList[i],recordInfoList[j]),
                datePrint)



