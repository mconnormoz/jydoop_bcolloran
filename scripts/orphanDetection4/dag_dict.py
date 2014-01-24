import json
import random

class Dag(object):
    def __init__(self,nodes,edges):
        self.nodeList=[]
        self.nodeDict=dict()
        if all( isinstance(newNode,Node) for newNode in nodes):
            self.add_nodes(nodes)
        else:
            raise
        if all( isinstance(newEdge,Edge) for newEdge in edges):
            self.edgeList=edges
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
        return sum( 1 for _ in self.edgesFromNode(node) )

    def edgesTo(self, node):
        return [edge for edge in self.edgeList if node.id==edge.target]

    def ancestorIds(self,node):
        return [edge.source.id for edge in self.edgesTo(node)]

    def ancestorNodes(self,node):
        return node.parents
        # return [ancNode for ancNode in self.nodeList if ancNode.id in self.ancestorIds(node)]

    def childIds(self,node):
        return [edge.target for edge in self.edgesFrom(node)]
    def childNodes(self,node):
        # we do a little footwork to 'memoize' this since it's slow
        try:
            return node.childNodes
        except AttributeError:
            node.childNodes = [chlNode for chlNode in self.nodeList if chlNode.id in self.childIds(node)]
            return node.childNodes

    def add_node(self, newNode):
        if self.hasNodeWithId(newNode):
            #if the node is in the graph, update it by combining
            # subclasses must implement combineNodes
            sameIdNode = next(node for node in self.nodeList if node.id==newNode.id)
            sameIdNode.combineNodes(newNode)
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
            print "self.getNodeById(newEdge.source.id)",repr(self.getNodeById(newEdge.source.id))
            self.getNodeById(newEdge.source.id).addChild(newEdge.target)
            self.getNodeById(newEdge.target.id).addParent(newEdge.source)


    def add_edges(self, edgeList):
        for edge in edgeList:
            self.add_edge(edge)
        # print "self.edgeList",self.edgeList
        print "self.edgeList[0].target",self.edgeList[0].target
        print "self.edgeList[0].target.children",self.edgeList[0].target.children

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
        self.parentIds=[]
        self.childIds=[]
    def __str__(self):
        return str(self.id)
    def __repr__(self):
        return str(self.id)
    def addChild(self,otherNode):
        self.childIds+=[otherNode]
    def addParent(self,otherNode):
        self.parentIds+=[otherNode]


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
        return json.dumps({"id":self.id,"source":self.source.id,"target":self.target.id})

    def __repr__(self):
        return json.dumps({"id":self.id,"source":self.source.id,"target":self.target.id})














class DayNode(Node):
    def __init__(self,id,date,data):
        Node.__init__(self,id)
        self.date = date
        self.width = None
        self.yOffset =None
        self.data = data
    def __str__(self):
        return json.dumps({"id":self.id,"date":self.date,"width":self.width,"yOffset":self.yOffset,"data":self.data})
    def __repr__(self):
        return repr({"id":self.id,"date":self.date,"yOffset":self.yOffset,"children":self.childIds,"parents":self.parentIds})
    def combineNodes(self,other):
        if self.date!=other.date:
            print self
            print other
            raise TypeError
        if self.data!=other.data:
            print self
            print other
            raise TypeError
        return self

class DayEdge(Edge):
    def __init__(self,id,source,target,count):
        self.id = id
        self.source=source
        self.target=target
        self.count = count
    def __str__(self):
        return json.dumps({"id":self.id,"source":self.source.id,"target":self.target.id,"count":self.count},cls=MyEncoder)
    def combineEdges(self,other):
        self.count+=other.count


class MyEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__

# MyEncoder().encode(f)
# '{"fname": "/foo/bar"}'

# Then you pass this class into the json.dumps() method as cls kwarg:

# json.dumps(cls=MyEncoder)










class DayDag(Dag):
    def __init__(self,nodes,edges):
        Dag.__init__(self,nodes,edges)
        self.top = self.getNodeById("top")
        self.bottom = self.getNodeById("bottom")

    def combineEdgeInfo(self,edgeInfo1,edgeInfo2):
        return edgeInfo1+edgeInfo2

    def combineNodes(self,edgeInfo1,edgeInfo2):
        return edgeInfo1+edgeInfo2

    def __str__(self):
        # print {"nodes":self.nodeList, "edges":self.edgeList, "maxDate":self.maxDate() }
        return json.dumps({"nodes":self.nodeList, "edges":self.edgeList, "maxDate":self.maxDate(), "minDate":self.minDate() },cls=MyEncoder)
    def __repr__(self):
        return json.dumps({"nodes":self.nodeList, "edges":self.edgeList, "maxDate":self.maxDate() })

    def maxNumOut(self):
        return max( self.numEdgesFromNode(nodeLabel) for nodeLabel,info in self.nodeList )

    def maxDate(self):
        try:
            return max( node.date for node in self.ancestorNodes(self.top) if node.date )
        except:
            print "parents", self.top.parents
            print "self.ancestorNodes(self.top)", self.ancestorNodes(self.top)
            print self.nodeList
            print self.edgeList
            raise ValueError

    def minDate(self):
        try:
            return min( node.date for node in self.childNodes(self.bottom) if node.date )
        except:
            print "parents", self.top.parents
            print "self.ancestorNodes(self.top)",self.ancestorNodes(self.top)
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

        queue = self.ancestorNodes(self.top)
        while queue:
            # print [node.id for node in queue]
            thisNode = queue.pop(0)
            if self._subtreeWidthsKnown(thisNode):
                # if all the child nodes have been handled, this node's width can be updated, and its parent can be put on the front of the queue
                thisNode.width = sum( node.width for node in self.childNodes(thisNode) )
                queue = self.ancestorNodes(thisNode)+queue
            else:
                #otherwise, put this node in the back of the queue; first, remove it if it's already in the queue
                queue = [node for node in queue if node!=thisNode]
                queue.append(thisNode)
        print "widths added"














if __name__=="__main__":
    nodesA=[Node(id=id) for i in range(10)]
    edgesA=[Edge(id=str(nodesA[i].id)+"_"+str(nodesA[i+1].id), source=nodesA[i].id, target=nodesA[i+1].id) for i in range(len(nodesA)-1)]
    print nodesA
    print edgesA
    dagA=Dag(nodesA,edgesA)
    print dagA

