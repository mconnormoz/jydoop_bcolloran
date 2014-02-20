import jydoopBatch
import socket
import datetime







if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
    batchEnv = jydoopBatch.env(jydoopRoot="/home/bcolloran/jydoop_bcolloran2/jydoop/",
        scriptRoot="scripts/orphanDetection4/",
        dataRoot="/user/bcolloran/orphanDetection4/divStats/",
        logPath="outData/orphIterLogs4/",
        verbose=True,
        onCluster=True)
        #HDFS paths
    initInDataPath = "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/part-r-000*"
    # "/tmp/full_dumb_export/part-m-*01"
    # "/tmp/full_dumb_export"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"
else:
    print "================ LOCAL RUN ================"
    rootPath = "/data/mozilla/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/orphanDetection4/",
        dataRoot=rootPath+"testData/fhrExtract_2014-02-03/",
        logPath="testData/fhrExtract_2014-02-03/",
        verbose=True,
        onCluster=False)

    initInDataPath = "/data/mozilla/jydoop_bcolloran/testData/sampleOfLinkedRecords_2014-02-03.fhrRaw"
    # "/data/mozilla/jydoop_bcolloran/testData/sampleOfRecordsWithOrphans_2013-11-05_1000rec.txt"
    # initInDataPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/testData/sampleOfFhrPacketsWithDuplicatedFingerprint_afterMultiDelete_2013-08-05.jydoopRaw"





# print "\n==== initialize graph parts"
# jydoopBatch.job(batchEnv,
#     "initRecordScan_kDocId_vPartOrTieBreakInfo.py",
#     initInDataPath,
#     "kDocId_vPartOrTieBreakInfo").run()


# print "\n==== find initial part overlaps, skip tieBreakInfo"
# numOverlapping = jydoopBatch.job(batchEnv,
#         "findTouchingDocsAndParts.py",
#         "kDocId_vPartOrTieBreakInfo",
#         "kPart_vObjTouchingPart_0")\
#     .run().getCounterVal("OVERLAPPING_PARTS")

# print "==initial number overlapping:",numOverlapping



# graphIter = 0
# print "\n================ iteration ================",graphIter
# while graphIter<10:
    
#     if numOverlapping==0:
#         convergedFlag=True
#         break

#     jydoopBatch.job(batchEnv,
#         script="relabelDocsWithLowestPart.py",
#         inPathList="kPart_vObjTouchingPart_"+str(graphIter),
#         outPath="kDoc_vPart_"+str(graphIter+1)).run()
#     graphIter+=1

#     print "\n==== check for overlaps",graphIter
#     numOverlapping = jydoopBatch.job(batchEnv,
#         "findTouchingDocsAndParts.py",
#         "kDoc_vPart_"+str(graphIter),
#         "kPart_vObjTouchingPart_"+str(graphIter+1))\
#     .run().getCounterVal("OVERLAPPING_PARTS")
#     graphIter+=1
#     print "==number overlapping:",numOverlapping



# if convergedFlag:
#     print "\n================ graph converged ================ iter:",graphIter,"\n"
#     batchEnv.log("\n================ graph converged ================ iter: "+str(graphIter)+"\n")
# else:
#     print "\n====== graph FAILED TO converge on iter:",graphIter
#     print "(some kind of error occurred)\n"
#     exit()



# print "==== join kPart_vObjTouchingPart_${finalIter} with initInData (kDocId,vRawFhr) -> kPart_vFhrJson"
# jydoopBatch.job(batchEnv,
#     "join_kDocIdVPartId_to_initInData.py",
#     ["kPart_vObjTouchingPart_"+str(graphIter),initInDataPath],
#     "kPartId_vFhrJson").run()



# # print "==== generate raw Session Divergence Graph file per part"
# # jydoopBatch.job(batchEnv,
# #     "kPartId_vSessionDivergenceInfo.py",
# #     "kPartId_vFhrJson",
# #     "kPartId_vDivGraph").run()


# # print "==== generate raw fhrJson and Session Divergence Graph file per part"
# # jydoopBatch.job(batchEnv,
# #     "fhrRawAndSessionDivergenceGraph_filePerPartId.py",
# #     ["kPartId_vDivGraph","kPartId_vFhrJson"],
# #     "docsAndDivGraphsPerPart/fhrJsonsWithDivGraph").run()


#running .5-1
#done: 0.5,1,2,3,4
print "==== generate Day Graph per doc"
jydoopBatch.job(batchEnv,
    "kPartId_vDayPosetGraph.py",
    "kPartId_vFhrJson_0.75-1",
    "kPartId_vDayPosetGraph").run()

print "==== generate Day Divergence graph per doc"
jydoopBatch.job(batchEnv,
    "kPartId_vDayDivergencePosetGraph.py",
    "kPartId_vFhrJson_0.75-1",
    "kPartId_vDayDivergencePosetGraph").run()

print "==== output one file per each partId with all the dayGraphs for each doc in the part, and the merged dayDivergencePosetGraph for the entire part"
jydoopBatch.job(batchEnv,
    "daysAndDayDivGraph_filePerPartId.py",
    ["kPartId_vDayPosetGraph","kPartId_vDayDivergencePosetGraph"],
    "graphFilePerPart/").run()


batchEnv.log("Batch complete: "+ datetime.datetime.utcnow().isoformat()+"\n")
batchEnv.logger.durationStamp().write().email()

