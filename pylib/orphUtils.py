import jydoop

def outputTabSep(path, results):
    # just dump tab separated key/vals
    firstLine = True
    with open(path, 'w') as f:
        for k, v in results:
            if firstLine:
                f.write(str(k)+"\t"+str(v))
                firstLine=False
            else:
                f.write("\n"+str(k)+"\t"+str(v))



def hdfsjobByType(hdfsType):

    def setupjob(job, args):
        """
        Set up a job to run on a list of paths.  Jobs expect at least one path,
        but you may specify as many as you like.
        """

        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
        import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat as MyInputFormat

        if len(args) < 1:
            raise Exception("Usage: path [ path2 ] [ path3 ] [ ... ]")

        job.setInputFormatClass(MyInputFormat)
        FileInputFormat.setInputPaths(job, ",".join(args));
        """Indicate to HadoopDriver which Mapper we want to use."""
        job.getConfiguration().set("org.mozilla.jydoop.mappertype", hdfsType)

    return setupjob


def localTextInput(evalVal=False,evalTup=False):
    def innerFunc(mapper):
        #local feeds a line of text input to the function after cleaning it up
        #just ignore the line key. split
        if jydoop.isJython(): #if we're on the cluster, return the mapper without doing anything
            return mapper
        else:
            def localMapper(lineKey,inputLine,context):
                keyValList = inputLine.split("\t")
                if evalTup:
                    outVal = eval(keyValList[1].strip()) if keyValList[1][0]=="("  else keyValList[1].strip()
                    return mapper(keyValList[0].strip(),outVal,context)
                elif evalVal:
                    return mapper(keyValList[0].strip(),eval(keyValList[1].strip()),context)
                else:
                    return mapper(keyValList[0].strip(),keyValList[1].strip(),context)
            return localMapper
    return innerFunc




# need to use this since python dicts don't guarantee order, and since json.dumps with sorting flag is broken in jydoop
def dictToSortedTupList(objIn):
    if isinstance(objIn,dict):
        return [(key,dictToSortedTupList(val)) for key,val in sorted(objIn.items(),key=lambda item:item[0])]
    else:
        return objIn


def dictToSortedJsonish(objIn):
    """ This is only designed to work on JSON-like structures that contain only: dicts, lists, strings, numbers. """
    if isinstance(objIn,dict):
        strOut=""
        for key,val in sorted(objIn.items(),key=lambda item:item[0]):
            strOut += "{" + str(key) + ":" + dictToSortedJsonish(val) + "}"
        return strOut
    else:
        return "{"+str(objIn)+"}"


class partSet(object):
    def __init__(self,partId):
        self.parts = set([partId])
    def __or__(self,other):
        return self.parts | other.parts

class docSet(object):
    def __init__(self,docId):
        self.docs = set([docId])
    def __or__(self,other):
        return self.docs | other.docs









