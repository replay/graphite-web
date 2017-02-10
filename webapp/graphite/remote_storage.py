import time
import httplib
import urllib3
from Queue import Queue
from urllib import urlencode
from threading import Lock, current_thread, Thread, ThreadError
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


class RemoteResultCompleteness(object):

  def __init__(self, store_count, timeout):
    self._sc_value = store_count
    self._sc_lock = Lock()
    self._await_complete = Lock()
    self._timed_out = False
    self._timeout_after(timeout)

    # if there are backend stores to wait for we acquire the lock to make
    # other threads wait
    if self._sc_value > 0:
      self._await_complete.acquire()

  # creates a thread that sets this object to timed out after the passed
  # number of seconds
  def _timeout_after(self, timeout):
    def _start_thread():
      def _timeout():
        time.sleep(timeout)
        self._timed_out = True
        try:
          self._await_complete.release()
        except ThreadError:
          # if the request has been completed before the timeout has been reached,
          # .release() will be called on an unlocked lock
          pass

      t = Thread(target=_timeout)
      t.daemon = True
      t.start()

    if settings.USE_WORKER_POOL:
      # we don't want to block a pool worker for the whole duration of REMOTE_FETCH_TIMEOUT
      # and we also don't want the overhead of creating a new thread in this thread, so we
      # make a pool worker create a new thread to wait for the timeout
      get_pool().put(
        job=(_start_thread,),
      )
    else:
      _start_thread()

  # waits until there are no backend stores left to wait for
  # if timeout has been reached the returned value is False
  def await_complete(self):
    self._await_complete.acquire()
    return not self._timed_out

  # called when a store has completed, decreases the number of stores
  # to wait for by one
  def store_completed(self):
    with self._sc_lock:
      sc_value = self._sc_value - 1
      self._sc_value -= sc_value

      # if we have no store left to wait for, release await_complete
      if sc_value == 0:
        self._await_complete.release()

    log.info(
      'RemoteReader:: Decreasing stores_left count by 1, new count is {count}'
      .format(count=sc_value),
    )


class RemoteStore(object):
  lastFailure = 0.0

  def __init__(self, host):
    self.host = host
    self.last_failure = 0

  @property
  def available(self):
    return time.time() - self.last_failure > settings.REMOTE_RETRY_DELAY

  def fail(self):
    self.last_failure = time.time()

  def find(self, query, headers=None):
    return list(FindRequest(self, query).send(headers))


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
  inflight_lock = Lock()

  def __init__(self, store, node_info, bulk_query=None):
    self.store = store
    self.metric_path = node_info.get('path') or node_info.get('metric_path')
    self.intervals = node_info['intervals']
    self.query = bulk_query or self.metric_path
    self.connection = None

  def __repr__(self):
    return '<RemoteReader[%x]: %s>' % (id(self), self.store.host)

  @staticmethod
  def _log(msg):
    log.info(('thread %s at %fs ' % (current_thread().name, time.time())) + msg)

  @classmethod
  def log_debug(cls, msg):
    if settings.DEBUG:
      cls._log(msg)

  @classmethod
  def log_error(cls, msg):
    cls._log(msg)

  def get_intervals(self):
    return self.intervals

  def fetch(self, startTime, endTime, now=None, requestContext=None):
    seriesList = self.fetch_list(startTime, endTime, now, requestContext)

    def _fetch(seriesList):
      if seriesList is None:
        return None

      for series in seriesList:
        if series['name'] == self.metric_path:
          time_info = (series['start'], series['end'], series['step'])
          return (time_info, series['values'])

      return None

    if isinstance(seriesList, FetchInProgress):
      return FetchInProgress(lambda: _fetch(seriesList.waitForResults()))

    return _fetch(seriesList)

  def fetch_list(self, startTime, endTime, now=None, requestContext=None):
    (startTime, endTime, now) = timebounds(requestContext)
    t = time.time()

    query_params = [
      ('target', self.query),
      ('format', 'pickle'),
      ('local', '1'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    if now is not None:
      query_params.append(('now', str( int(now) )))

    query_string = urlencode(query_params)
    urlpath = '/render/'
    url = "%s://%s%s" % ('https' if settings.INTRACLUSTER_HTTPS else 'http', self.store.host, urlpath)
    headers = requestContext.get('forwardHeaders') if requestContext else None

    cacheKey = "%s?%s" % (url, query_string)

    with self.inflight_lock:
      self.log_debug("RemoteReader:: Got global lock %s?%s" % (url, query_string))
      if requestContext is None:
        requestContext = {}
      if 'inflight_locks' not in requestContext:
        requestContext['inflight_locks'] = {}
      if 'inflight_requests' not in requestContext:
        requestContext['inflight_requests'] = {}
      if cacheKey not in requestContext['inflight_locks']:
        self.log_debug("RemoteReader:: Creating lock %s?%s" % (url, query_string))
        requestContext['inflight_locks'][cacheKey] = Lock()

    cacheLock = requestContext['inflight_locks'][cacheKey]
    result_completeness = requestContext.get('result_completeness')

    with cacheLock:
      self.log_debug("RemoteReader:: got url lock %s?%s" % (url, query_string))

      if cacheKey in requestContext['inflight_requests']:
        self.log_debug("RemoteReader:: Returning cached FetchInProgress %s?%s" % (url, query_string))
        return requestContext['inflight_requests'][cacheKey]

      q = Queue()
      if settings.USE_WORKER_POOL:
        get_pool().put(
          job=(self._fetch, url, query_string, query_params, headers),
          result_queue=q,
        )
      else:
        q.put(
          self._fetch(url, query_string, query_params, headers),
        )

      def retrieve():
        with retrieve.lock:
          # if the result is known we return it directly
          if hasattr(retrieve, '_result'):
            self.log_error('RemoteReader:: retrieve completed (cached) %s' % (', '.join([result['path'] for result in getattr(retrieve, '_result')])))
            return getattr(retrieve, '_result')

          # otherwise we get it from the queue and keep it for later
          results = q.get(block=True)

          if results is not None:
            for i in range(len(results)):
              results[i]['path'] = results[i]['name']
          else:
            self.log_error('RemoteReader:: retrieve has received no results')

          setattr(retrieve, '_result', results)
          self.log_error('RemoteReader:: retrieve completed %s' % (', '.join([result['path'] for result in results])))
          return results

      self.log_debug(
        'RemoteReader:: Storing FetchInProgress with cacheKey {cacheKey}'
        .format(cacheKey=cacheKey),
      )
      retrieve.lock = Lock()
      data = FetchInProgress(retrieve)
      requestContext['inflight_requests'][cacheKey] = data

      if result_completeness is not None:
        result_completeness.store_completed()

      self.log_debug("RemoteReader:: Returning %s?%s in %fs" % (url, query_string, time.time() - t))
      return data

  def _fetch(self, url, query_string, query_params, headers):
    self.log_debug("RemoteReader:: Starting to execute _fetch %s?%s" % (url, query_string))
    try:
      self.log_debug("ReadResult:: Requesting %s?%s" % (url, query_string))
      result = http.request(
        'POST' if settings.REMOTE_STORE_USE_POST else 'GET',
        url,
        fields=query_params,
        headers=headers,
        timeout=settings.REMOTE_FIND_TIMEOUT,
      )

      if result.status != 200:
        self.store.fail()
        self.log_error("ReadResult:: Error response %d from %s?%s" % (result.status, url, query_string))
        data = None
      else:
        data = unpickle.loads(result.data)
    except Exception as err:
      self.store.fail()
      self.log_error("ReadResult:: Error requesting %s?%s: %s" % (url, query_string, err))
      data = None

    self.log_debug("RemoteReader:: Completed _fetch %s?%s" % (url, query_string))
    return data


def extractForwardHeaders(request):
    headers = {}
    for name in settings.REMOTE_STORE_FORWARD_HEADERS:
        headers[name] = request.META.get('HTTP_%s' % name.upper().replace('-', '_'))
    return headers
