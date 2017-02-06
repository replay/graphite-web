

class Node(object):
  __slots__ = ('name', 'path', 'local', 'is_leaf')

  def __init__(self, path):
    self.path = path
    self.name = path.split('.')[-1]
    self.local = True
    self.is_leaf = False

  def __repr__(self):
    return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)


class BranchNode(Node):
  pass


class LeafNode(Node):
  __slots__ = ('reader', 'intervals')

  def __init__(self, path, reader):
    Node.__init__(self, path)
    self.reader = reader
    self.is_leaf = True

  def fetch(self, startTime, endTime, requestContext=None):
    try:
      return (self, self.reader.fetch(startTime, endTime, requestContext))
    except TypeError:
      return (self, self.reader.fetch(startTime, endTime))

  def __repr__(self):
    return '<LeafNode[%x]: %s (%s)>' % (id(self), self.path, self.reader)
