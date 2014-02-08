import jydoopBatch
import socket
import datetime







if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
 
    batchEnv = jydoopBatch.env(jydoopRoot="/home/bcolloran/jydoop_bcolloran2/jydoop/",
        scriptRoot="scripts/orphanDetection4.1/",
        dataRoot="/user/bcolloran/orphanDetection4/test_fullExport__2014-01-31/",
        logPath="outData/orphIterLogs4.1/",
        verbose=True,
        onCluster=True)
        #HDFS paths
    initInDataPath = "/user/bcolloran/data/fhrFullDump_2014-01-31"
    #"/tmp/full_dumb_export"
    # "/tmp/full_dumb_export/part-m-*01"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/part-r-0001*"
    # "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/"
else:
    print "================ LOCAL RUN ================"
    rootPath = "/data/mozilla/jydoop_bcolloran/"
    batchEnv = jydoopBatch.env(jydoopRoot=rootPath,
        scriptRoot="scripts/linkedRecordSample/",
        dataRoot=rootPath+"testData/orph4.1/",
        logPath="testData/orph4.0/",
        verbose=True,
        onCluster=False)

    initInDataPath = "/data/mozilla/jydoop_bcolloran/testData/sampleOfRecordsWithOrphans_2013-11-05_1000rec.txt"
    # initInDataPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/testData/sampleOfFhrPacketsWithDuplicatedFingerprint_afterMultiDelete_2013-08-05.jydoopRaw"







print '''this job requires an already existing
kPart_vObjTouchingPart_${finalIter}
in the dataRoot dir
'''


print "\n==== get naive head doc id for each part"
jydoopBatch.job(batchEnv,
    "sampleFromFinalPartIds.py",
    "kPart_vObjTouchingPart_2",
    "linkedSample_kDocId_vPartId").run()




batchEnv.log("Batch complete: "+ datetime.datetime.utcnow().isoformat()+"\n")
batchEnv.logger.durationStamp().write().email()