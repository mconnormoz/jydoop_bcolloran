import jydoopBatch
import socket
import datetime







if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
 
    batchEnv = jydoopBatch.env(jydoopRoot="/home/bcolloran/jydoop_bcolloran2/jydoop/",
        scriptRoot="scripts/orphanDetection3/",
        dataRoot="/user/bcolloran/orphanDetection3/test1/",
        logPath="outData/orphIterLogs3/",
        verbose=True,
        onCluster=True)
        #HDFS paths
    initInDataPath = "/tmp/full_dumb_export/part-m-0000*"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"    #"/tmp/full_dumb_export"
else:
    print "================ LOCAL RUN ================"
    rootPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/orphanDetection3/",
        dataRoot=rootPath+"testData/orph3.0/",
        logPath="testData/orph3.0/",
        verbose=True,
        onCluster=False)

    initInDataPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/testData/orph2.5/sampleOfRecordsWithOrphans_2013-11-05_3.txt"








print "\n==== initialize graph parts"
jydoopBatch.job(batchEnv,
    "initPartsByDateDataPrint.py",
    initInDataPath,
    "kDoc_vPart_0").run()





graphIter = 0
print "\n================ iteration ================",graphIter
while graphIter<100:
    print "\n==== check for overlaps",graphIter
    numOverlapping = jydoopBatch.job(batchEnv,
        "findTouchingDocsAndParts.py",
        "kDoc_vPart_"+str(graphIter),
        "kPart_vObjTouchingPart_"+str(graphIter+1))\
    .run().getCounterVal("OVERLAPPING_PARTS")
    graphIter+=1

    print "==number overlapping:",numOverlapping
    if numOverlapping==0:
        convergedFlag=True
        break

    jydoopBatch.job(batchEnv,
        script="relabelDocsWithLowestPart.py",
        inPathList="kPart_vObjTouchingPart_"+str(graphIter),
        outPath="kDoc_vPart_"+str(graphIter+1)).run()
    graphIter+=1

if convergedFlag:
    print "\n================ graph converged ================ iter:",graphIter,"\n"
    batchEnv.log("\n================ graph converged ================ iter: "+str(graphIter)+"\n")
else:
    print "\n====== graph FAILED TO converge on iter:",graphIter
    print "(some kind of error occurred)\n"
    exit()








print "==== get partIds for each docId"
jydoopBatch.job(batchEnv,
    "final_kDocId_vPartId.py",
    "kPart_vObjTouchingPart_"+str(graphIter),
    "kDocId_vPartId_final").run()


print "==== get tie breaker info for each doc"
jydoopBatch.job(batchEnv,
    "kDocId_vTieBreakInfo_fromRawJsons.py",
    initInDataPath,
    "kDocId_vTieBreakInfo").run()




print "==== take initial Jsons and kDocId_vPartId_final to kPart_vFhrJson"
jydoopBatch.job(batchEnv,
    "kPartId_vRawJson.py",
    ["kDocId_vPartId_final",initInDataPath],
    "kPartId_vFhrJson").run()

print "\n==== get the naive tie breaker info for each record, label it by part"
jydoopBatch.job(batchEnv,
    "kPartId_vDocId-tieBreakInfo.py",
    ["kDocId_vPartId_final",initInDataPath],
    "kPartId_vDocId-tieBreakInfo").run()


# next take the tie breaker info for each part, and emit the head doc id for that part
print "\n==== get naive head doc id for each part"
jydoopBatch.job(batchEnv,
    "final_kNaiveHeadRecordDocId_vPartId.py",
    "kPartId_vDocId-tieBreakInfo",
    "final_kNaiveHeadRecordDocId_vPart").run()



batchEnv.log("Batch complete: "+ datetime.datetime.utcnow().isoformat()+"\n")
batchEnv.logger.durationStamp().write().email()

