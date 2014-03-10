import jydoopBatch
import socket
import datetime
import os



extractDate=datetime.datetime.utcnow().isoformat()[0:10]



if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
 
    batchEnv = jydoopBatch.env(jydoopRoot="/home/bcolloran/jydoop_bcolloran2/jydoop/",
        scriptRoot="scripts/orphanDetection4.1/",
        dataRoot="/user/bcolloran/data/fhrDeorphaning_"+extractDate+"/",
        logPath="outData/orphIterLogs4.1/",
        verbose=True,
        onCluster=True,
        batchName="naive head record extraction")
        #HDFS paths
    initInDataPath = "/user/bcolloran/data/fhrFullExtract_"+extractDate
    #"/tmp/full_dumb_export"
    # "/tmp/full_dumb_export/part-m-*01"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/part-r-0001*"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"
    # 
else:
    print "================ LOCAL RUN ================"
    rootPath = "/data/mozilla/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/orphanDetection4.1/",
        dataRoot=rootPath+"testData/orph4.1/",
        logPath="testData/orph4.0/",
        verbose=True,
        onCluster=False,
        batchName="naive head record extraction")

    initInDataPath = "/data/mozilla/jydoop_bcolloran/testData/sampleOfRecordsWithOrphans_2013-11-05_1000rec.txt"
    # initInDataPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/testData/sampleOfFhrPacketsWithDuplicatedFingerprint_afterMultiDelete_2013-08-05.jydoopRaw"





'''
to make a snapshot on peach:
cd /home/bcolloran/pig
pig -param OUTPUT=/user/bcolloran/data/fhrFullExtract_%s hbase_export.pig -D pig.additional.jars=./elephant-bird-core-4.3.jar:./elephant-bird-hadoop-compat-4.3.jar:./elephant-bird-pig-4.3.jar
'''
if batchEnv.onCluster:
    os.chdir("/home/bcolloran/pig/")
    commandList = ["pig","-param","OUTPUT=/user/bcolloran/data/fhrFullExtract_%s"%extractDate,"hbase_export.pig","-D","pig.additional.jars=./elephant-bird-core-4.3.jar:./elephant-bird-hadoop-compat-4.3.jar:./elephant-bird-pig-4.3.jar"]
    command = "pig -param OUTPUT=/user/bcolloran/data/fhrFullExtract_%s hbase_export.pig -D pig.additional.jars=./elephant-bird-core-4.3.jar:./elephant-bird-hadoop-compat-4.3.jar:./elephant-bird-pig-4.3.jar"%extractDate
    jydoopBatch.runCommand(batchEnv,command)
    os.chdir("/home/bcolloran/jydoop_bcolloran2/jydoop/")








print "\n==== initialize graph parts"
jydoopBatch.job(batchEnv,
    "initRecordScan_kDocId_vPartOrTieBreakInfo.py",
    initInDataPath,
    "kDocId_vPartOrTieBreakInfo").run()


print "\n==== find initial part overlaps, skip tieBreakInfo"
numOverlapping = jydoopBatch.job(batchEnv,
        "findTouchingDocsAndParts.py",
        "kDocId_vPartOrTieBreakInfo",
        "kPart_vObjTouchingPart_0")\
    .run().getCounterVal("OVERLAPPING_PARTS")

print "==initial number overlapping:",numOverlapping



graphIter = 0
print "\n================ iteration ================",graphIter
while graphIter<10:
    
    if numOverlapping==0:
        convergedFlag=True
        break

    jydoopBatch.job(batchEnv,
        script="relabelDocsWithLowestPart.py",
        inPathList="kPart_vObjTouchingPart_"+str(graphIter),
        outPath="kDoc_vPart_"+str(graphIter+1)).run()
    graphIter+=1

    print "\n==== check for overlaps",graphIter
    numOverlapping = jydoopBatch.job(batchEnv,
        "findTouchingDocsAndParts.py",
        "kDoc_vPart_"+str(graphIter),
        "kPart_vObjTouchingPart_"+str(graphIter+1))\
    .run().getCounterVal("OVERLAPPING_PARTS")
    graphIter+=1
    print "==number overlapping:",numOverlapping



if convergedFlag:
    print "\n================ graph converged ================ iter:",graphIter,"\n"
    batchEnv.log("\n================ graph converged ================ iter: "+str(graphIter)+"\n")
else:
    print "\n====== graph FAILED TO converge on iter:",graphIter
    print "(some kind of error occurred)\n"
    exit()




'''
now that the graph has converged, we have:
kDocId_vPartOrTieBreakInfo
kPart_vObjTouchingPart_${finalIter}

we want final_kNaiveHeadRecordDocId_vPart

to get there,
final_kNaiveHeadRecordDocId_vPart needs INPUT:
    kPartId_vDocId-tieBreakInfo
with vDocId-tieBreakInfo like: ("docId",(thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime))

to get kPartId_vDocId-tieBreakInfo, need to join 
    kDocId_vPartOrTieBreakInfo
with
    kPart_vObjTouchingPart_${finalIter}
'''






# graphIter=2 ###################### WARNING!!!!




print "==== join kDocId_vPartOrTieBreakInfo with kPart_vObjTouchingPart_${finalIter}"
jydoopBatch.job(batchEnv,
    "join_kDocIdVPartId_toTieBreakinfo.py",
    ["kDocId_vPartOrTieBreakInfo","kPart_vObjTouchingPart_"+str(graphIter)],
    "kPartId_vDocId-tieBreakInfo").run()




'''
next take the tie breaker info for each part, and emit the head doc id for that part

    input must be:
      k: "partId"
      v: ("docId",(thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime))
'''




if batchEnv.onCluster:
    """this has to be run "by hand" when on peach, because jydoop requires relative path names to save locally (to peach-gw).
ex:
make ARGS="scripts/orphanDetection4.1/final_kNaiveHeadRecordDocId_vPartId.py outData/orphanDetection4/fullExport_2014-02-27_final_naiveHeadRecordDocId.txt /user/bcolloran/data/fhrDeorphaning_2014-02-27/kPartId_vDocId-tieBreakInfo" hadoop"""
    
    jydoopBatch.runCommand(batchEnv,
        ["make",
        'ARGS="scripts/orphanDetection4.1/final_kNaiveHeadRecordDocId_vPartId.py outData/orphanDetection4/fullExport_%s_final_naiveHeadRecordDocId.txt /user/bcolloran/data/fhrDeorphaning_%s/kPartId_vDocId-tieBreakInfo"'%(extractDate,extractDate),
        "hadoop"])
else:
    '''if running locally, the absolute'''
    print "\n==== get naive head doc id for each part"
    jydoopBatch.job(batchEnv,
        "final_kNaiveHeadRecordDocId_vPartId.py",
        "kPartId_vDocId-tieBreakInfo",
        "final_naiveHeadRecordDocId.txt").run()




if batchEnv.onCluster:
    '''
#if on the cluster:
# then load to HDFS
hdfs dfs -put /home/bcolloran/jydoop_bcolloran2/jydoop/outData/orphanDetection4/fullExport_2014-02-27_final_naiveHeadRecordDocId.txt /user/bcolloran/data/fhrDeorphaning_2014-02-27/headRecordsFinalDocIds_2014-02-27.txt

# then extract Head record docs with pig script
pig -param orig=/user/bcolloran/data/fhrFullExtract_2014-02-27/ -param fetchids=/user/bcolloran/data/fhrDeorphaning_2014-02-27/headRecordsFinalDocIds_2014-02-27.txt -param jointype=merge -param output=fhrDeorphaned_2014-02-27 fetch_reports.aphadke.pig
'''
    jydoopBatch.runCommand(batchEnv,
        ["hdfs","dfs","-put",
        "/home/bcolloran/jydoop_bcolloran2/jydoop/outData/orphanDetection4/fullExport_%s_final_naiveHeadRecordDocId.txt"%extractDate,
        "/user/bcolloran/data/fhrDeorphaning_%s/headRecordsFinalDocIds.txt"%extractDate])
    os.chdir("/home/bcolloran/pig/")
    jydoopBatch.runCommand(batchEnv,
        ["pig",
        "-param", "orig=/user/bcolloran/data/fhrFullExtract_%s/"%extractDate,
        "-param", "fetchids=/user/bcolloran/data/fhrDeorphaning_%s/headRecordsFinalDocIds.txt"%extractDate,
        "-param", "jointype=merge",
        "-param", "output=fhrDeorphaned_%s"%extractDate,
        "fetch_reports.aphadke.pig"])






batchEnv.log("Batch complete: "+ datetime.datetime.utcnow().isoformat()+"\n")
batchEnv.logger.durationStamp().write().email()

