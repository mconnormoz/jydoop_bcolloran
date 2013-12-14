import jydoopBatch







if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
 
    batchEnv = jydoopBatch.env(jydoopRoot=,"/home/bcolloran/jydoop_bcolloran2/jydoop/"
        scriptRoot="scripts/orphanDetection3",
        dataRoot="/user/bcolloran/orphanDetection3/test1/",
        logPath="outData/orphIterLogs3/"
        verbose=True,
        onCluster=True)
        #HDFS paths
    initInDataPath = "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"    #"/tmp/full_dumb_export"
else:
    print "================ LOCAL RUN ================"
    rootPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/orphanDetection3",
        dataRoot=rootPath+"testData/orph2.7/",
        logPath="outData/orphIterLogs3/"
        verbose=True,
        onCluster=False)

    initInDataPath = rootPath+"testData/orph2.5/"+"sampleOfRecordsWithOrphans_2013-11-05_3.txt"
    fileDriverPath=rootPath+"FileDriver.py"

    dataPath = rootPath+"testData/orph2.7/"
    logPath = dataPath









print "\n==== initialize graph parts"
jydoopBatch.job(batchEnv,"getWeightsAndInitPartsFromRecords.py", initInDataPath,"kEdge_vPart_0").run()


graphIter = 0
print "\n================ iteration ================",graphIter
while graphIter<100:
    print "\n==== check for overlaps",graphIter
    numOverlapping = jydoopBatch.job(batchEnv,
        script="edgeAndPartOverlaps.py",
        inPathList="kEdge_vPart_"+str(graphIter),
        outPath="kPart_vObjTouchingPart_"+str(graphIter+1))\
    .run().getCounterVal("OVERLAPPING_PARTS")
    graphIter+=1
    print "==number overlapping:",numOverlapping
    if numOverlapping==0:
        convergedFlag=True
        break

    jydoopBatch.job(batchEnv,
        script="relabelEdges.py",
        inPathList="kPart_vObjTouchingPart_"+str(graphIter),
        outPath="kEdge_vPart_"+str(graphIter+1)).run()
    graphIter+=1



if convergedFlag:
    print "\n================ graph converged ================ iter:",graphIter,"\n"
    logger.log("\n================ graph converged ================ iter: "+graphIter+"\n")
else:
    print "\n====== graph FAILED TO converge on iter:",graphIter
    print "(some kind of error occurred)\n"
    exit()



