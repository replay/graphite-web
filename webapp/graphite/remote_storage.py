import time
import httplib
import requests
from urllib import urlencode
from django.conf import settings
from django.core.cache import cache
from graphite.node import LeafNode, BranchNode
from graphite.logger import log
from graphite.util import unpickle
from graphite.render.hashing import compactHash
from graphite.intervals import Interval, IntervalSet

def connector_class_selector(https_support=False):
    return httplib.HTTPSConnection if https_support else httplib.HTTPConnection


class RemoteStore(object):
  lastFailure = 0.0
  available = property(lambda self: time.time() - self.lastFailure > settings.REMOTE_RETRY_DELAY)

  def __init__(self, host):
    self.host = host

  def find(self, query, result_queue=False, headers=None):
    request = FindRequest(self, query)
    result = request.send(headers)
    if result_queue:
      result_queue.put(result)
    else:
      return result

  def fail(self):
    self.lastFailure = time.time()


class FindRequest(object):
  __slots__ = ('store', 'query', 'cacheKey')

  def __init__(self, store, query):
    self.store = store
    self.query = query

    if query.startTime:
      start = query.startTime - (query.startTime % settings.FIND_CACHE_DURATION)
    else:
      start = ""

    if query.endTime:
      end = query.endTime - (query.endTime % settings.FIND_CACHE_DURATION)
    else:
      end = ""

    self.cacheKey = "find:%s:%s:%s:%s" % (store.host, compactHash(query.pattern), start, end)

  def send(self, headers=None):
    log.info("FindRequest.send(host=%s, query=%s) called" % (self.store.host, self.query))

    results = cache.get(self.cacheKey)
    if results is not None:
      log.info("FindRequest(host=%s, query=%s) using cached result" % (self.store.host, self.query))
    else:
      url = "%s://%s/metrics/find/" % ('https' if settings.INTRACLUSTER_HTTPS else 'http', self.store.host)

      query_params = [
        ('local', '1'),
        ('format', 'pickle'),
        ('query', self.query.pattern),
      ]
      if self.query.startTime:
        query_params.append( ('from', self.query.startTime) )

      if self.query.endTime:
        query_params.append( ('until', self.query.endTime) )

      query_string = urlencode(query_params)

      try:
        if settings.REMOTE_STORE_USE_POST:
          result = requests.post(url, data=query_params, headers=headers or {}, timeout=settings.REMOTE_FIND_TIMEOUT)
        else:
          result = requests.get(url, params=query_params, headers=headers or {}, timeout=settings.REMOTE_FIND_TIMEOUT)
      except:
        log.exception("FindRequest.send(host=%s, query=%s) exception during request" % (self.store.host, self.query))
        self.store.fail()
        return

      try:
        result.raise_for_status()
        results = unpickle.loads(result.content)
      except:
        log.exception("FindRequest.get_results(host=%s, query=%s) exception processing response" % (self.store.host, self.query))
        self.store.fail()
        return

      cache.set(self.cacheKey, results, settings.FIND_CACHE_DURATION)

    for node_info in results:
      # handle both 1.x and 0.9.x output
      path = node_info.get('path') or node_info.get('metric_path')
      is_leaf = node_info.get('is_leaf') or node_info.get('isLeaf')
      intervals = node_info.get('intervals') or []
      if not isinstance(intervals, IntervalSet):
        intervals = IntervalSet([Interval(interval[0], interval[1]) for interval in intervals])

      node_info = {
        'is_leaf': is_leaf,
        'path': path,
        'intervals': intervals,
      }

      if is_leaf:
        reader = RemoteReader(self.store, node_info, bulk_query=self.query.pattern)
        node = LeafNode(path, reader)
      else:
        node = BranchNode(path)

      node.local = False
      yield node


class RemoteReader(object):
  __slots__ = ('store', 'metric_path', 'intervals', 'query', 'connection')

  def __init__(self, store, node_info, bulk_query=None):
    self.store = store
    self.metric_path = node_info['path']
    self.intervals = node_info['intervals']
    self.query = bulk_query or node_info['path']
    self.connection = None

  def __repr__(self):
    return '<RemoteReader[%x]: %s>' % (id(self), self.store.host)

  def get_intervals(self):
    return self.intervals

  def fetch_list(self, startTime, endTime, now=None, headers=None):
    query_params = [
      ('target', self.query),
      ('format', 'pickle'),
      ('local', '1'),
      ('noCache', '1'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    if now is not None:
      query_params.append(('now', str( int(now) )))

    query_string = urlencode(query_params)
    urlpath = '/render/'
    url = "%s://%s%s" % ('https' if settings.INTRACLUSTER_HTTPS else 'http', self.store.host, urlpath)

    try:
      log.info("ReadResult :: requesting %s?%s" % (url, query_string))
      if settings.REMOTE_STORE_USE_POST:
        result = requests.post(url, data=query_params, headers=headers, timeout=settings.REMOTE_FETCH_TIMEOUT)
      else:
        result = requests.get(url, params=query_params, headers=headers, timeout=settings.REMOTE_FETCH_TIMEOUT)
    except:
      self.store.fail()
      log.exception("Error requesting %s?%s" % (url, query_string))
      raise

    if result.status_code != 200:
      raise Exception("Error response %d %s from %s?%s" % (result.status_code, result.reason, url, query_string))

    return unpickle.loads(result.content)

  def fetch(self, startTime, endTime, now=None, headers=None):
    seriesList = self.fetch_list( startTime, endTime, now, headers);

    for series in seriesList:
      if series['name'] == self.metric_path:
        time_info = (series['start'], series['end'], series['step'])
        return (time_info, series['values'])

    return None


def extractForwardHeaders(request):
    headers = {}
    for name in settings.REMOTE_STORE_FORWARD_HEADERS:
        headers[name] = request.META.get('HTTP_%s' % name.upper().replace('-', '_'))
    return headers
