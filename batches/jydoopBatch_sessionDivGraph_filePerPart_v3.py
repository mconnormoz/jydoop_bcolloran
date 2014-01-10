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
    initInDataPath = "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"    #"/tmp/full_dumb_export"
else:
    print "================ LOCAL RUN ================"
    rootPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/orphanDetection3/",
        dataRoot=rootPath+"testData/orph3.0/",
        logPath="testData/orph3.0/",
        verbose=True,
        onCluster=False)

    initInDataPath = "../orph2.5/"+"sampleOfRecordsWithOrphans_2013-11-05_3.txt"






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





print "==== take initial Jsons and kDocId_vPartId_final to kPart_vFhrJson"
jydoopBatch.job(batchEnv,
    "../orphanDetection2/kPartId_vRawJson.py",
    ["kDocId_vPartId_final",initInDataPath],
    "kPartId_vFhrJson").run()

# jydoopJob(scriptPath+"kPartId_vDocId-RawJson.py",
#                 [dataPath+"kDocId_vPartId_final",initInDataPath],
#                 dataPath+"kPartId_vRawJson").run()

# # Now we can bin these (partId,(docId,fhrJson)) pairs by partId see which of the jsons in each part is a possible head record, and emit the final set of (docId,fhrJson) pairs. or we can generate divergence graphs.


print "==== generate kPartId_vSessionDivergenceGraph"
jydoopBatch.job(batchEnv,
    "../orphanDetection2/kPartId_vSessionDivergenceGraph.py",
    "kPartId_vFhrJson",
    "kPartId_vSessionDivergenceGraph").run()


print "==== generate raw fhrJson and Session Divergence Graph file per part"
jydoopBatch.job(batchEnv,
    "../orphanDetection2/fhrRawAndSessionDivergenceGraph_perFile.py",
    ["kPartId_vFhrJson","kPartId_vSessionDivergenceGraph"],
    "docsAndDivGraphsPerPart/docsAndDivGraph").run()


batchEnv.log("Batch complete: "+ datetime.datetime.utcnow().isoformat()+"\n")
batchEnv.logger.durationStamp().write().email()

