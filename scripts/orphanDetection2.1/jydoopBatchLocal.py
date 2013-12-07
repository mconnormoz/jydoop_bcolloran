import subprocess
import re
import socket

# jydoopLocal -t testData/orph2.5/sampleOfRecordsWithOrphans_2013-11-05_3.txt -o testData/orph2.5/kEdge_vPart_0 scripts/orphanDetection2/getWeightsAndInitPartsFromRecords.py

# "python FileDriver.py $pathToScript $TESTDATA $OUTFILE"


if socket.gethostname()=='peach-gw.peach.metrics.scl3.mozilla.com':
    onCluster=True
    rootPath = "/home/bcolloran/jydoop_bcolloran2/jydoop/"
else:
    onCluster=False
    rootPath = "/home/bcolloran/Desktop/projects/jydoop_bcolloran/"



dataPath = rootPath+"testData/orph2.5/"
scriptPath = rootPath+"scripts/orphanDetection2/"
fileDriverPath=rootPath+"FileDriver.py"



class jydoopRunner(object):
    """docstring for ClassName"""
    def __init__(self,scriptPath,inPathList,outPath):
        self.script = scriptPath
        self.inPathList = inPathList
        self.outPath = outPath
        self.stdout=None
        self.stderr=None

    def run(self):
        if onCluster:
            pass
        else:
            commandList = ["python", fileDriverPath, self.script, self.inPathList, self.outPath]
            p = subprocess.Popen(commandList,stdout=subprocess.PIPE)
            stdout,stderr = p.communicate()
            print stdout
            self.stdout=stdout
            self.stderr=stderr









print "==== initialize graph parts"
# initScriptPath = scriptPath+"getWeightsAndInitPartsFromRecords.py"
# initInDataPath = dataPath+"sampleOfRecordsWithOrphans_2013-11-05_3.txt"
# initOutDataPath = dataPath+"kEdge_vPart_0"
initRunner = jydoopRunner(scriptPath+"getWeightsAndInitPartsFromRecords.py",dataPath+"sampleOfRecordsWithOrphans_2013-11-05_3.txt",dataPath+"kEdge_vPart_0")
initRunner.run()

# initCommandList = ["python", fileDriverPath, initScriptPath,initInDataPath, initOutDataPath]

# p = subprocess.Popen(initCommandList,stdout=subprocess.PIPE)
# stdout,stderr = p.communicate()
# print stdout

graphIter = 0



partOverlapScriptPath = scriptPath+"edgeAndPartOverlaps.py"
partOverlapInDataPath = dataPath+"kEdge_vPart_"
partOverlapOutDataPath = dataPath+"kPart_vObjTouchingPart_"

relabelEdgesScriptPath = scriptPath+"relabelEdges.py"
relabelEdgesInDataPath = dataPath+"kPart_vObjTouchingPart_"
relabelEdgesOutDataPath = dataPath+"kEdge_vPart_"

while graphIter<10:
    print "================ iteration ================",graphIter
    print "==== check for overlaps",graphIter

    partOverlapCommand = ["python", fileDriverPath, partOverlapScriptPath, partOverlapInDataPath+str(graphIter), partOverlapOutDataPath+str(graphIter+1)]

    partOverlapCommandProc = subprocess.Popen(partOverlapCommand,stdout=subprocess.PIPE)
    stdout,stderr = partOverlapCommandProc.communicate()
    print stdout
    overlappingPartsInfo = re.findall("INFO mapred.JobClient:\s+OVERLAPPING_PARTS=[0-9]+",stdout)
    print overlappingPartsInfo
    print "==number overlapping:",overlappingPartsInfo[0].split("=")[1]
    graphIter+=1
    if overlappingPartsInfo[0].split("=")[1]=="0":
        break


    print "==== relabel all edges with lowest partId",graphIter
    relabelEdgesCommand = ["python", fileDriverPath, relabelEdgesScriptPath,relabelEdgesInDataPath+str(graphIter),relabelEdgesOutDataPath+str(graphIter+1)]
    
    relabelEdgesCommandProc = subprocess.Popen(relabelEdgesCommand,stdout=subprocess.PIPE)
    stdout,stderr = relabelEdgesCommandProc.communicate()
    print stdout
    graphIter+=1

print "================ graph converged ================ iter:",graphIter


# at this point, we have a file full of (kPart,vObjTouchingPart) pairs, in which all of the vObjTouchingPart items will be lists of weighted edges between documents.
# to find the head record of each of these sets of records, we need to go back and look at the records themselves again, which means we have to join the (docId,fhrJson) pairs with (docId,partId) pairs, so that we can flip this around to get (partId,(docId,fhrJson)) pairs.

print "==== get partIds for each docId"
docIdPerPartScriptPath = scriptPath+"final_kDocId_vPartId.py"
docIdPerPartInDataPath = dataPath+"kPart_vObjTouchingPart_"+str(graphIter)
docIdPerPartOutDataPath = dataPath+"kDocId_vPartId_final"

docIdPerPartCommandList = ["python", fileDriverPath, docIdPerPartScriptPath,docIdPerPartInDataPath, docIdPerPartOutDataPath]

p = subprocess.Popen(docIdPerPartCommandList,stdout=subprocess.PIPE)
stdout,stderr = p.communicate()
print stdout


print "==== concatenate kDocId_vPartId and kDocId_vFhrJson files"

with open(dataPath+"kDocId_vPartIdOrJson","w") as outfile:#outfile
    with open(docIdPerPartOutDataPath,"r") as infile: #kDocId_vPartId file
        for line in infile:
            outfile.write(line)
    outfile.write("\n")
    with open(initInDataPath,"r") as infile: #kDocId_vFhrJson file
        for line in infile:
            outfile.write(line)


print "==== take kDocId_vPartIdOrJson to kPart_vFhrJson"
partIdPerJsonScriptPath = scriptPath+"kPartId_vRawJson.py"
partIdPerJsonInDataPath = dataPath+"kDocId_vPartIdOrJson"
partIdPerJsonOutDataPath = dataPath+"kPartId_vFhrJson"

partIdPerJsonCommandList = ["python", fileDriverPath, partIdPerJsonScriptPath,partIdPerJsonInDataPath, partIdPerJsonOutDataPath]

p = subprocess.Popen(partIdPerJsonCommandList,stdout=subprocess.PIPE)
stdout,stderr = p.communicate()
print stdout



# Now, then we can bin these (partId,(docId,fhrJson)) pairs by partId see which of the jsons in each part is a possible head record, and emit the final set of (docId,fhrJson) pairs. or we can generate divergence graphs






