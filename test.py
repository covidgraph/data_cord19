from py2neo import Graph, Node, Relationship
from py2neo.ogm import GraphObject
from DZDpy2neoUtils.ogm import GraphObjectUtils

n1 = Node("Label1", name="Tim", _id="1")

n2 = Node("Label1", _id="1")
n1.__primarylabel__ = "Label1"
n1.__primarykey__ = "_id"
n2.__primarylabel__ = "Label1"
n2.__primarykey__ = "_id"


n3 = Node("Label2", _id="2")
n3.__primarylabel__ = "Label2"
n3.__primarykey__ = "_id"

rel = Relationship(n3, "REL", n2)
g = Graph()

go = GraphObject.wrap(n1)
print(GraphObjectUtils.get_graphobject_property(go))
# g.merge(n1)
# g.create(rel)
# g.merge(n2)
