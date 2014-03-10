import subprocess
import re
import datetime
import os
import smtplib




class logger(object):
    def __init__(self,env,initString=""):
        self.logPath=env.logPath
        self.env=env

        try:
            os.makedirs(self.logPath)
        except OSError as exception:
            if not os.path.isdir(self.logPath):
                raise
        self.logString=initString
        self.initTime = datetime.datetime.utcnow()
    def log(self,logEntry):
        self.logString += logEntry
        return self
    def __repr__(self):
        return self.logString
    def durationStamp(self):
        self.logString += "\nElapsed time: "+ str(datetime.datetime.utcnow()-self.initTime)
        # + " seconds ("+str((datetime.datetime.utcnow()-self.initTime).total_seconds()/3600)+ "hrs)"
        return self
    def write(self):
        with open(self.logPath+"log_"+datetime.datetime.utcnow().isoformat(),"w") as logFile:
            logFile.write(str(self.logString))
        return self
    def email(self,success=True):
        if self.env.onCluster:
            sender = 'bcolloran@mozilla.com'
            receivers = ['bcolloran@mozilla.com']
            succOrFail= "SUCCESS" if success else "FAILED"
            message = """From: jydoop batch bot <bcolloran@mozilla.com>
To: <bcolloran@mozilla.com>
Subject: -%s- %s

%s %s. Jydoop batch logs follow.
------------------------------------------

"""%(succOrFail,self.env.batchName,self.env.batchName,succOrFail)


            try:
                smtpObj = smtplib.SMTP('localhost')
                smtpObj.sendmail(sender, receivers, message+self.logString)         
                print "Successfully sent email"
            except SMTPException:
                print "Error: unable to send email"
            return self
        else:
            print "email not sent for local jobs."














class env(object):
    def __init__(self,jydoopRoot,scriptRoot,dataRoot,logPath=None,verbose=False,onCluster=False,
        batchName="(no batch name given)"):
        self.jydoopRoot=jydoopRoot
        self.scriptRoot=scriptRoot
        self.dataRoot=dataRoot
        self.verbose=verbose
        self.onCluster=onCluster
        self.logPath=logPath
        self.batchName=batchName
        if logPath:
            self.logger=logger(self)
        else:
            self.logger=None

    def log(self,msg):
        if self.logger:
            self.logger.log(msg)
        else:
            print "WARNING: no logger created for this batch environment"
















class job(object):
    """docstring for ClassName"""
    def __init__(self,batchEnv,script,inPathList,outPath):
        # needs paths to scripts, inData, outData, which are given as relative paths from batchEnv.scriptRoot, batchEnv.dataRoot ,batchEnv.dataRoot (respectively)

        # self.onCluster = batchEnv.onCluster
        # self.jydoopRoot = batchEnv.jydoopRoot
        self.script = script if script[0]=="/" else batchEnv.scriptRoot + script
        if type(inPathList)==type([]):
                self.inPathList = [(path if path[0]=="/" else batchEnv.dataRoot + path) for path in inPathList]
        else:
            self.inPathList = inPathList if inPathList[0]=="/" else batchEnv.dataRoot + inPathList
        self.outPath = batchEnv.dataRoot + outPath
        self.logger = batchEnv.logger
        self.env = batchEnv


        self.stdout=None
        self.stderr=None

        self.methodChainer()

    def methodChainer(self):
        return self

    def __mergeFilestoTmpPath(self):
        tmpFile = self.env.dataRoot+"tmp_"+ datetime.datetime.utcnow().isoformat()
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
        if self.env.onCluster:
            reMatches = re.findall("INFO mapred.JobClient:\s+"+counterName+"=[0-9]+",self.stderr)
        else:
            reMatches = re.findall("INFO mapred.JobClient:\s+"+counterName+"=[0-9]+",self.stdout)
        return int(reMatches[0].split("=")[-1]) #last string match value


    def run(self):
        if self.env.onCluster:
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
            commandList = ["python", self.env.jydoopRoot+"FileDriver.py", self.script, inPath, self.outPath]
            command = " ".join(commandList)
            p = subprocess.Popen(commandList,stdout=subprocess.PIPE)

        retcode = p.wait()
        stdout,stderr = p.communicate()

        if self.logger:
            logger=self.logger
            logger.log("\n======= Command issued:  "+command)
            logger.log("\n         ===stdout==="+(("\n"+stdout) if stdout else " None\n"))
            logger.log("\n         ===stderr==="+(("\n"+stderr) if stderr else " None\n"))
        if retcode: #process returns 0 on success
            print "\n         ===stdout===\n",stdout
            print "\n         ===stderr===\n",stderr
            print
            if self.logger:
                logger.log("\n\nBATCH FAILED :-(\n\n")
                logger.write().email(success=False)
            raise subprocess.CalledProcessError(retcode, " ".join(commandList))
        if self.env.verbose:
            print "\n         ===stdout===\n",stdout
            print "\n         ===stderr===\n",stderr

        self.stdout=stdout
        self.stderr=stderr

        return self




def runCommand(batchEnv,command):
    print "\nCommand issued:\n",command
    p = subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    retcode = p.wait()
    stdout,stderr = p.communicate()
    if batchEnv.logger:
        logger=batchEnv.logger
        logger.log("\n======= Command issued:  "+command)
        logger.log("\n         ===stdout==="+(("\n"+stdout) if stdout else " None\n"))
        logger.log("\n         ===stderr==="+(("\n"+stderr) if stderr else " None\n"))
    if retcode: #process returns 0 on success
        print "\n         ===stdout===\n",stdout
        print "\n         ===stderr===\n",stderr
        print
        if batchEnv.logger:
            logger.log("\n\nBATCH FAILED :-(\n\n")
            logger.write().email(success=False)
        raise subprocess.CalledProcessError(retcode, command)
    if self.env.verbose:
        print "\n         ===stdout===\n",stdout
        print "\n         ===stderr===\n",stderr









