import jydoopBatch
import socket
import datetime







if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
    batchEnv = jydoopBatch.env(jydoopRoot="/home/bcolloran/jydoop_bcolloran2/jydoop/",
        scriptRoot="scripts/orphanDetection4.1/",
        dataRoot="/user/bcolloran/orphanDetection4.1/divStats/",
        logPath="outData/orphIterLogs4.1/",
        verbose=True,
        onCluster=True)
        #HDFS paths
    initInDataPath = "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/part-r-0001*"
    # "/tmp/full_dumb_export/part-m-0001*"
    # "/tmp/full_dumb_export"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/part-r-0001*"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"
else:
    print "================ LOCAL RUN ================"
    rootPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/orphanDetection4.1/",
        dataRoot=rootPath+"testData/orph4.1/divStats/",
        logPath="testData/orph4.1/divStats/",
        verbose=True,
        onCluster=False)

    initInDataPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/testData/sampleOfRecordsWithOrphans_2013-11-05_1000rec.txt"
    # initInDataPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/testData/sampleOfFhrPacketsWithDuplicatedFingerprint_afterMultiDelete_2013-08-05.jydoopRaw"





print "\n==== initialize graph parts"
jydoopBatch.job(batchEnv,
    "initRecordScan_kDocId_vPartOrDayGraphInfo.py",
    initInDataPath,
    "kDocId_vPartOrDayGraphInfo").run()


print "\n==== find initial part overlaps, skip tieBreakInfo"
numOverlapping = jydoopBatch.job(batchEnv,
        "findTouchingDocsAndParts.py",
        "kDocId_vPartOrDayGraphInfo",
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



print "==== join kPart_vObjTouchingPart_${finalIter} with kDocId_vPartOrDayGraphInfo -> kPart_vFhrJson"
jydoopBatch.job(batchEnv,
    "join_kDocIdVPartId_to_dayGraphInfo.py",
    ["kPart_vObjTouchingPart_"+str(graphIter),"kDocId_vPartOrDayGraphInfo"],
    "kPartId_vDayGraphInfo").run()


print "==== generate divergence stats per part"
jydoopBatch.job(batchEnv,
    "kPartId_vDayDivergenceStats.py",
    "kPartId_vDayGraphInfo",
    "kPartId_vPartStats").run()



batchEnv.log("Batch complete: "+ datetime.datetime.utcnow().isoformat()+"\n")
batchEnv.logger.durationStamp().write().email()

