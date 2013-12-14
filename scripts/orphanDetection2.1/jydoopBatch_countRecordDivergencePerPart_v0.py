import subprocess
import re
import socket
import datetime
import os




if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    print "================ PEACH RUN ================"
    onCluster=True
    rootPath = "/home/bcolloran/jydoop_bcolloran2/jydoop/"
    #HDFS paths
    dataPath = "/user/bcolloran/orphanDetection2/test2/"
    initInDataPath = "/user/bcolloran/data/samples/fhr/v2/withOrphans/2013-11-05/part-r-0001*"
else:
    print "================ LOCAL RUN ================"
    onCluster=False
    rootPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/"
    dataPath = rootPath+"testData/orph2.7/"
    initInDataPath = rootPath+"testData/orph2.5/"+"sampleOfRecordsWithOrphans_2013-11-05_3.txt"
    fileDriverPath=rootPath+"FileDriver.py"
    os.mkdir(dataPath)




os.chdir(rootPath)
# MUST use a relative path from rootPath because of jydoop makefile weirdness
scriptPath = "scripts/orphanDetection2/" 






class jydoopJob(object):
    """docstring for ClassName"""
    def __init__(self,script,inPathList,outPath):
        self.script = script
        self.inPathList = inPathList
        self.outPath = outPath
        self.stdout=None
        self.stderr=None

        self.methodChainer()

    def methodChainer(self):
        return self

    def __mergeFilestoTmpPath(self):
        tmpFile = dataPath+"tmp_"+ datetime.datetime.utcnow().isoformat()
        print tmpFile
        firstLine = True

        with open(tmpFile,"w") as outfile:#outfile
            for inPath in self.inPathList:
                with open(inPath,"r") as infile:
                    for line in infile:
                        lineOut = line.strip() if firstLine else "\n"+line.strip()
                        outfile.write(lineOut)
                        firstLine=False
        return tmpFile

    def getCounterVal(self,counterName):
        if onCluster:
            reMatches = re.findall("INFO mapred.JobClient:\s+"+counterName+"=[0-9]+",self.stderr)
        else:
            reMatches = re.findall("INFO mapred.JobClient:\s+"+counterName+"=[0-9]+",self.stdout)
        return int(reMatches[0].split("=")[1]) #get the first string match value


    def run(self):
        if onCluster:
            if type(self.inPathList)==type([]):
                inPaths = " ".join(list(self.inPathList))
            else:
                inPaths = self.inPathList
            makeString = 'ARGS="%(script)s %(outPath)s %(inPaths)s"' % {"script": self.script,"outPath": self.outPath, "inPaths": inPaths}
            commandList = ["make",makeString,"hadoop"]
            command = " ".join(commandList)
            print "\nCommand issued:\n",command
            p = subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        else:
            if type(self.inPathList)==type([]):
                #in this case, concatenate these files to a temp file.
                inPath = self.__mergeFilestoTmpPath()
            else:
                inPath = self.inPathList
            commandList = ["python", fileDriverPath, self.script, inPath, self.outPath]
            p = subprocess.Popen(commandList,stdout=subprocess.PIPE)

        retcode = p.wait()
        stdout,stderr = p.communicate()
        
        # print "====== stdout ========================",stdout
        # print "====== stdout end =============================="

        # print "====== stderr ========================",stderr
        # print "====== stderr end =============================="

        if retcode: #process returns 0 on success
            raise subprocess.CalledProcessError(retcode, " ".join(commandList))
        self.stdout=stdout
        self.stderr=stderr
        return self

















print "==== initialize graph parts"
jydoopJob( scriptPath+"getWeightsAndInitPartsFromRecords.py" , initInDataPath,dataPath+"kEdge_vPart_0").run()


graphIter = 0
print "================ iteration ================",graphIter
while graphIter<10:
    print "==== check for overlaps",graphIter

    numOverlapping = jydoopJob(
            scriptPath+"edgeAndPartOverlaps.py",
            dataPath+"kEdge_vPart_"+str(graphIter),
            dataPath+"kPart_vObjTouchingPart_"+str(graphIter+1))\
        .run().getCounterVal("OVERLAPPING_PARTS")
    graphIter+=1
    print "==number overlapping:",numOverlapping
    if numOverlapping==0:
        break

    jydoopJob(scriptPath+"relabelEdges.py",
                    dataPath+"kPart_vObjTouchingPart_"+str(graphIter),
                    dataPath+"kEdge_vPart_"+str(graphIter+1)).run()
    graphIter+=1

print "================ graph converged ================ iter:",graphIter,"\n"


# at this point, we have a file full of (kPart,vObjTouchingPart) pairs, in which all of the vObjTouchingPart items will be lists of weighted edges between documents.
# to find the head record of each of these sets of records, we need to go back and look at the records themselves again, which means we have to join the (docId,fhrJson) pairs with (docId,partId) pairs, so that we can flip this around to get (partId,(docId,fhrJson)) pairs.

print "==== get partIds for each docId"

jydoopJob(scriptPath+"final_kDocId_vPartId.py",
                dataPath+"kPart_vObjTouchingPart_"+str(graphIter),
                dataPath+"kDocId_vPartId_final").run()


print "==== take initial Jsons and kDocId_vPartId_final to kPart_vFhrJson"
jydoopJob(scriptPath+"kPartId_vDocId-RawJson.py",
                [dataPath+"kDocId_vPartId_final",initInDataPath],
                dataPath+"kPartId_vDocId-RawJson").run()

# Now we can bin these (partId,(docId,fhrJson)) pairs by partId see which of the jsons in each part is a possible head record, and emit the final set of (docId,fhrJson) pairs. or we can generate divergence graphs.

print "==== get naive head-records as (kDocId,vFhrJson) pairs"
# jydoopJob(scriptPath+"naiveHeadrecordExtraction.py",
#                 dataPath+"kPartId_vFhrJson",
#                 dataPath+"kPartId_vSessionDivergenceGraph").run()


# print "==== generate kPartId_vSessionDivergenceGraph"
# jydoopJob(scriptPath+"kPartId_vSessionDivergenceGraph.py",
#                 dataPath+"kPartId_vFhrJson",
#                 dataPath+"kPartId_vSessionDivergenceGraph").run()


# print "==== generate fhrRawAndSessionDivergenceGraph_perFile"
# jydoopJob(scriptPath+"fhrRawAndSessionDivergenceGraph_perFile.py",
#                 [dataPath+"kPartId_vSessionDivergenceGraph", dataPath+"kPartId_vFhrJson"],
#                 dataPath+"/filePerPart/docAndDivgGraphs").run()





