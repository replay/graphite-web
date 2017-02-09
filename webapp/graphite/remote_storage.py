import time
import httplib
import urllib3
from Queue import Queue
from urllib import urlencode
from threading import Lock, current_thread
from django.conf import settings
from django.core.cache import cache
from graphite.intervals import Interval, IntervalSet
from graphite.node import LeafNode, BranchNode
from graphite.logger import log
from graphite.util import unpickle
from graphite.readers import FetchInProgress
from graphite.render.hashing import compactHash
from graphite.util import timebounds
from graphite.worker_pool.pool import get_pool

http = urllib3.PoolManager(num_pools=10, maxsize=5)

def connector_class_selector(https_support=False):
    return httplib.HTTPSConnection if https_support else httplib.HTTPConnection


class RemoteStore(object):
  lastFailure = 0.0
  available = property(
    lambda self: time.time() - self.last_failure > settings.REMOTE_RETRY_DELAY
  )

  def __init__(self, host):
    self.host = host
    self._failure_lock = Lock()
    self._last_failure = 0

  @property
  def last_failure(self):
    with self._failure_lock:
      return self._last_failure

  def find(self, query, result_queue=False, headers=None):
    request = FindRequest(self, query)
    if result_queue:
      result_queue.put(request.send(headers))
    else:
      return list(request.send(headers))

  def fail(self):
    with self._failure_lock:
      self._last_failure = time.time()


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
    t = time.time()
    log.info("FindRequest.send(host=%s, query=%s) called at %s" % (self.store.host, self.query, t))

    results = cache.get(self.cacheKey)
    if results is not None:
      log.info("FindRequest.send(host=%s, query=%s) using cached result" % (self.store.host, self.query))
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

      try:
        result = http.request('POST' if settings.REMOTE_STORE_USE_POST else 'GET',
                              url, fields=query_params, headers=headers, timeout=settings.REMOTE_FIND_TIMEOUT)
      except:
        log.exception("FindRequest.send(host=%s, query=%s) exception during request" % (self.store.host, self.query))
        self.store.fail()
        return

      if result.status != 200:
        log.exception("FindRequest.send(host=%s, query=%s) error response %d from %s?%s" % (self.store.host, self.query, result.status, url, urlencode(query_params)))
        self.store.fail()
        return

      try:
        results = unpickle.loads(result.data)
      except:
        log.exception("FindRequest.send(host=%s, query=%s) exception processing response" % (self.store.host, self.query))
        self.store.fail()
        return

      cache.set(self.cacheKey, results, settings.FIND_CACHE_DURATION)

    log.info("FindRequest.send(host=%s, query=%s) completed in %fs at %s" % (self.store.host, self.query, time.time() - t, time.time()))

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
    self.metric_path = node_info.get('path') or node_info.get('metric_path')
    self.intervals = node_info['intervals']
    self.query = bulk_query or self.metric_path
    self.connection = None

  def __repr__(self):
    return '<RemoteReader[%x]: %s>' % (id(self), self.store.host)

  def log_info(self, msg):
    log.info(('thread %s at %fs ' % (current_thread().name, time.time())) + msg)

  def get_intervals(self):
    return self.intervals

  def fetch(self, startTime, endTime, now=None, requestContext=None):
    reader_context = self.get_reader_context(
      startTime,
      endTime,
      now,
      requestContext,
    )

    series_list = self.fetch_list(reader_context)

    def _fetch(series_list):
      if series_list is None:
        return None

      for series in series_list:
        if series['name'] == self.metric_path:
          time_info = (series['start'], series['end'], series['step'])
          return (time_info, series['values'])

      return None

    if isinstance(series_list, FetchInProgress):
      return FetchInProgress(lambda: _fetch(series_list.waitForResults()))

    return _fetch(series_list)

  # the reader context is specific to request and reader,
  # so it's scope is smaller than that of the requestContext
  def get_reader_context(self, startTime, endTime, now, requestContext):
    query_params = [
      ('target', self.query),
      ('format', 'pickle'),
      ('local', '1'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    url_path = '/render/'
    url = '{proto}://{host}{url}'.format(
      proto='https' if settings.INTRACLUSTER_HTTPS else 'http',
      host=self.store.host,
      url=url_path,
    )

    if now is not None:
      query_params.append(('now', str( int(now) )))

    if 'inflight_lock' not in requestContext:
      requestContext['inflight_lock'] = Lock()

    query_string = urlencode(query_params)

    reader_context = {
      'cache_key': "%s?%s" % (
        url,
        query_string,
      ),
      'query_params': query_params,
      'request_context': requestContext if requestContext is not None else {},
      'urlpath': url_path,
      'url': url,
      'query_string': query_string,
    }

    reader_context['headers'] = requestContext.get('forwardHeaders', None)

    return reader_context

  def fetch_list(self, reader_context):
    request_context = reader_context['request_context']
    (startTime, endTime, now) = timebounds(request_context)
    t = time.time()

    if settings.REMOTE_PREFETCH_DATA and 'fetch_q' in request_context:
      self.fetch_into_result_queue(reader_context)
    else:
      self.fetch_into_request_context(reader_context)

    self.log_info("RemoteReader:: Returning %s?%s in %fs" % (
      reader_context['url'],
      reader_context['query_string'],
      time.time() - t,
    ))

  def fetch_into_result_queue(self, reader_context):
    requestContext = reader_context['request_context']
    if settings.USE_THREADING:
      get_pool().put(
        job=(
          lambda: self.add_path_to_results(
            self._fetch(reader_context),
          ),
        ),
        result_queue=requestContext['fetch_q'],
      )
    else:
      requestContext['fetch_q'].put(
        self._fetch(reader_context),
      )

  def fetch_into_request_context(self, reader_context):
    requestContext = reader_context['request_context']
    cacheKey = reader_context['cache_key']

    self.log_info(
      "RemoteReader:: Got global lock %s?%s" % (
        reader_context['url'],
        reader_context['query_string']
      ),
    )

    requestContext['inflight_lock'].acquire()

    inflight_contexts = requestContext.get('inflight_contexts')
    if inflight_contexts is None:
      inflight_contexts = {}
      requestContext['inflight_context'] = inflight_contexts

    inflight_context = inflight_contexts.get(cacheKey)
    if inflight_context is None:
      inflight_context = {}
      inflight_contexts[cacheKey] = inflight_context

    if 'lock' not in inflight_context:
      self.log_info("RemoteReader:: Creating lock %s?%s" % (
        reader_context['url'],
        reader_context['query_string'],
      ))
      inflight_context['lock'] = Lock()

    # reduce lock scope by switching from request context wide lock
    # to cacheKey specific one
    with inflight_context['lock']:
      requestContext['inflight_lock'].relase()

      self.log_info("RemoteReader:: got cacheKey lock %s" % cacheKey)

      if 'requests' in inflight_context:
        self.log_info(
          "RemoteReader:: Returning cached FetchInProgress %s?%s" % (
            reader_context['url'],
            reader_context['query_string'],
          ),
        )
        return inflight_context['requests']

      q = Queue()
      if settings.USE_THREADING:
        get_pool().put(
          job=(self._fetch, reader_context),
          result_queue=q,
        )
      else:
        q.put(
          self._fetch(reader_context),
        )

      data = self.retrieve_into_FetchInProgress(q)
      self.log_info(
        'RemoteReader:: Storing FetchInProgress with cacheKey {cacheKey}'
        .format(cacheKey=cacheKey),
      )

      inflight_context['requests'] = data
      return data

  @staticmethod
  def add_path_to_results(results):
    for i in range(len(results)):
      results[i] = (results[i]['name'], results[i])
    return results

  def retrieve_into_FetchInProgress(self, q):
    def _retrieve():
      with _retrieve.lock:
        # if the result is known we return it directly
        if hasattr(_retrieve, '_result'):
          return getattr(_retrieve, '_result')

        # otherwise we get it from the queue and keep it for later
        results = q.get(block=True)

        if results is not None:
          self.add_path_to_results(results)
        else:
          self.log_info('RemoteReader:: _retrieve has received no results')

        setattr(_retrieve, '_result', results)
        return results

    _retrieve.lock = Lock()
    return FetchInProgress(_retrieve)

  def _fetch(self, reader_context):
    requestContext = reader_context['request_context']
    self.log_info(
      'RemoteReader:: Starting to execute _fetch %s?%s' % (
        reader_context['url'],
        reader_context['query_string'],
      ),
    )
    try:
      self.log_info("ReadResult:: Requesting %s?%s" % (
          reader_context['url'],
          reader_context['query_string'],
        ),
      )
      result = http.request(
        'POST' if settings.REMOTE_STORE_USE_POST else 'GET',
        reader_context['url'],
        fields=reader_context['query_params'],
        headers=reader_context['headers'],
        timeout=settings.REMOTE_FIND_TIMEOUT,
      )
      if result.status != 200:
        self.store.fail()
        self.log_info(
          'ReadResult:: Error response %d from %s?%s' % (
            result.status,
            reader_context['url'],
            reader_context['query_string'],
          ),
        )
        data = None
      else:
        data = unpickle.loads(result.data)
    except Exception as err:
      self.store.fail()
      self.log_info("ReadResult:: Error requesting %s?%s: %s" % (reader_context['url'], reader_context['query_string'], err))
      data = None

    self.log_info("RemoteReader:: Completed _fetch %s?%s" % (reader_context['url'], reader_context['query_string']))
    return data


def extractForwardHeaders(request):
    headers = {}
    for name in settings.REMOTE_STORE_FORWARD_HEADERS:
        headers[name] = request.META.get('HTTP_%s' % name.upper().replace('-', '_'))
    return headers
