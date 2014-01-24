import json
import random

class Dag(object):
    def __init__(self,nodes,edges):
        self.nodeList=[]
        self.edgeList=[]
        self.nodeDict=dict()
        if all( isinstance(newNode,Node) for newNode in nodes):
            self.add_nodes(nodes)
        else:
            raise
        if all( isinstance(newEdge,Edge) for newEdge in edges):
            self.add_edges(edges)
        else:
            raise

    def __repr__(self):
        return json.dumps({"nodes":self.nodeList,"edges":self.edgeList})
    def __str__(self):
        return json.dumps({"nodes":self.nodeList,"edges":self.edgeList})

    def edgeInfo(self,edgeLabel):
        return self.edgesDict[edgeLabel]

    def hasNodeWithId(self,otherId):
        return otherId in self.nodeDict

    def getNodeById(self, nodeId):
        return self.nodeDict[nodeId]

    def hasEdgeWithSameId(self,otherEdge):
        return (otherEdge.id in [edge.id for edge in self.edgeList])

    def edgesFrom(self, node):
        return [edge for edge in self.edgeList if node.id==edge.source.id]

    def numEdgesFromNode(self, node):
        return sum( 1 for _ in self.edgesFrom(node) )

    def edgesTo(self, node):
        return [edge for edge in self.edgeList if node.id==edge.target]

    # def ancestorIds(self,node):
    #     return [edge.source.id for edge in self.edgesTo(node)]

    def parentNodes(self,node):
        return [self.nodeDict[nodeId] for nodeId in node.parentIds]


    # def childIds(self,node):
    #     return [edge.target for edge in self.edgesFrom(node)]
    def childNodes(self,node):
        for nodeId in node.childIds:
            yield self.nodeDict[nodeId]


    def add_node(self, newNode):
        if self.hasNodeWithId(newNode.id):
            #if the node is in the graph, update it by combining
            # subclasses must implement mergeNode
            sameIdNode = next(node for node in self.nodeList if node.id==newNode.id)
            sameIdNode.mergeNode(newNode)
        else:
            self.nodeList+=[newNode]
            self.nodeDict[newNode.id]=newNode

    def add_nodes(self, nodeList):
        for node in nodeList:
            self.add_node(node)

    def add_edge(self, newEdge):
        if self.hasEdgeWithSameId(newEdge):
            #if the edge is in the graph, update it by combining
            # subclasses must implement combineEdges
            sameIdEdge = [edge for edge in self.edgeList if edge.id==newEdge.id][0]
            sameIdEdge.combineEdges(newEdge)
        else:
            #add it to edgeList and update linked nodes
            self.edgeList+=[newEdge]
            # print "newEdge",newEdge
            self.getNodeById(newEdge.source.id).addChildId(newEdge.target.id)
            self.getNodeById(newEdge.target.id).addParentId(newEdge.source.id)

    def add_edges(self, edgeList):
        for edge in edgeList:
            self.add_edge(edge)

    def merge(self, other):
        """
        merge other graph with this graph.
        """
        self.add_nodes( other.nodeList )
       
        for node in other.nodeList:
            self.add_edges( other.edgesFrom(node) )








class Node(object):
    def __init__(self,id):
        self.id = id
        self.parentIds=set()
        self.childIds=set()
    def __str__(self):
        return str(self.__dict__)
    def __repr__(self):
        return str(self.__dict__)
    def addChildId(self,otherNodeId):
        self.childIds.add(otherNodeId)
    def addParentId(self,otherNodeId):
        self.parentIds.add(otherNodeId)
    def mergeNode(self,otherNode):
        self.parentIds.update(otherNode.parentIds)
        self.childIds.update(otherNode.childIds)



class Edge(object):
    def __init__(self,id,source,target):
        self.id = id
        if isinstance(source,Node):
            self.source=source
        else:
            raise
        if isinstance(target,Node):
            self.target=target
        else:
            raise
        
    def __str__(self):
        return json.dumps({"id":self.id,"sourceId":self.source.id,"targetId":self.target.id})

    def __repr__(self):
        return json.dumps({"id":self.id,"sourceId":self.source.id,"targetId":self.target.id})














class DayNode(Node):
    def __init__(self,id,date,maxDateInThread,data):
        Node.__init__(self,id)
        self.date = date
        self.width = 1
        self.yOffset =None
        self.nodeCount = 1
        self.data = data
        self.maxDateInThread = maxDateInThread
    def __str__(self):
        return json.dumps({"id":self.id,"date":self.date,"width":self.width,"yOffset":self.yOffset,"data":self.data},cls=MyEncoder)
    def __repr__(self):
        return json.dumps({"id":self.id,"date":self.date,"yOffset":self.yOffset})
    def __json__(self):
        return {"id":self.id,"date":self.date,"yOffset":self.yOffset,"width":self.width }
        # ,"childIds":self.childIds,"parentIds":self.parentIds})
    def mergeNode(self,other):
        Node.mergeNode(self,other)
        if self.date!=other.date:
            print self
            print other
            raise TypeError
        if self.data!=other.data:
            print self
            print other
            raise TypeError
        self.width += other.width
        self.maxDateInThread = max(self.maxDateInThread,other.maxDateInThread)
        # print self.width, other.width

class DayEdge(Edge):
    def __init__(self,id,source,target,count):
        self.id = id
        self.source=source
        self.target=target
        self.count = count
    def __str__(self):
        return json.dumps({"id":self.id,"sourceId":self.source.id,"targetId":self.target.id,"count":self.count},cls=MyEncoder)
    def __repr__(self):
        return json.dumps({"id":self.id,"sourceId":self.source.id,"targetId":self.target.id,"count":self.count},cls=MyEncoder)
    def __json__(self):
        return {"id":self.id,"sourceId":self.source.id,"targetId":self.target.id,"count":self.count}
    def combineEdges(self,other):
        self.count+=other.count



class MyEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__json__()







class DayDag(Dag):
    def __init__(self,nodes,edges):
        Dag.__init__(self,nodes,edges)
        self.top = self.getNodeById("top")
        self.bottom = self.getNodeById("bottom")

    def combineEdgeInfo(self,edgeInfo1,edgeInfo2):
        return edgeInfo1+edgeInfo2

    # def mergeNode(self,edgeInfo1,edgeInfo2):
    #     return edgeInfo1+edgeInfo2

    def __str__(self):
        # print {"nodes":self.nodeList, "edges":self.edgeList, "maxDate":self.maxDate() }
        return json.dumps({"nodes":self.nodeList, "edges":self.edgeList, "maxDate":self.maxDate(), "minDate":self.minDate() },cls=MyEncoder)
    def __repr__(self):
        return json.dumps({"nodes":self.nodeList, "edges":self.edgeList, "maxDate":self.maxDate() },cls=MyEncoder)

    def maxNumOut(self):
        return max( self.numEdgesFromNode(nodeLabel) for nodeLabel,info in self.nodeList )

    def maxDate(self):
        try:
            return max( node.date for node in self.parentNodes(self.top) if node.date )
        except:
            print "parents", self.top.parentIds
            print "self.parentNodes(self.top)", self.parentNodes(self.top)
            print self.nodeList
            print self.edgeList
            raise ValueError

    def minDate(self):
        try:
            return min( node.date for node in self.childNodes(self.bottom) if node.date )
        except:
            print self.nodeList
            print self.edgeList
            raise ValueError

    def addYOffset(self):
        for node in self.nodeList:
            node.yOffset=random.random()

    def _subtreeWidthsKnown(self,node):
        #subtree width is initialized as None, will be set when known;
        return all(child.width for child in self.childNodes(node))

    def addSubtreeWidths(self):
        # setting the width of top to "1" initializes the tree widths
        self.top.width = 1

        queue = self.parentNodes(self.top)
        while queue:
            # print [node.id for node in queue]
            thisNode = queue.pop(0)
            if self._subtreeWidthsKnown(thisNode):
                # if all the child nodes have been handled, this node's width can be updated, and its parent can be put on the front of the queue
                thisNode.width = sum( node.width for node in self.childNodes(thisNode) )
                queue = self.parentNodes(thisNode)+queue
            else:
                #otherwise, put this node in the back of the queue; first, remove it if it's already in the queue
                queue = [node for node in queue if node!=thisNode]
                queue.append(thisNode)
        print "widths added"

    def addSubtreeWidths_2(self):
        # setting the width of top to "1" initializes the tree widths
        self.top.width = 1

        queue = self.parentNodes(self.top)
        while queue:
            # print [node.id for node in queue]
            thisNode = queue.pop(0)
            try: 
                # if all the child nodes have been handled, then node.width will be a number for all of them this node's width can be updated, and its parent can be put on the front of the queue
                thisNode.width = sum( node.width for node in self.childNodes(thisNode) )
                queue = self.parentNodes(thisNode)+queue
            except:
                #otherwise, put this node in the back of the queue; first, remove it if it's already in the queue
                queue = [node for node in queue if node!=thisNode]
                queue.append(thisNode)
        print "widths added"

    def addYOffset(self):
        #starting from the first session in the chain, add the thread offset by going down the tree.
        #at each node, set the offset of the CHILD nodes, then add those child nodes to the queue.
        #to initialize, set the offset of the initial sessions, and then add them to the queue

        initialNodes = sorted(self.childNodes(self.bottom),key=lambda node: node.maxDateInThread, reverse=True)
        queue=[]
        offsets=0
        # find the offsets of the initialSessions
        for node in initialNodes:
            node.yOffset = offsets #will be 0
            offsets+=node.width
            queue.append(node) #add this node to the queue, so that its children can be checked in the while loop
        while(queue):
            print queue
            thisNode = queue.pop(0)
            children = sorted(self.childNodes(self.bottom),key=lambda thisNode: thisNode.maxDateInThread, reverse=True)
            try:
                offsets=thisNode.yOffset
            except:
                print thisNode, self.sessionChain[thisNode]
                raise
            for childNode in children:
                childNode.yOffset = offsets
                offsets+=childNode.width
                queue.append(childNode)













if __name__=="__main__":
    nodesA=[Node(id=id) for i in range(10)]
    edgesA=[Edge(id=str(nodesA[i].id)+"_"+str(nodesA[i+1].id), source=nodesA[i].id, target=nodesA[i+1].id) for i in range(len(nodesA)-1)]
    print nodesA
    print edgesA
    dagA=Dag(nodesA,edgesA)
    print dagA

