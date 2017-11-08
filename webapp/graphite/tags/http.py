from __future__ import absolute_import

from urllib import quote
import json

from django.conf import settings
from graphite.http_pool import http

from graphite.tags.base import BaseTagDB


class HttpTagDB(BaseTagDB):
  """
  Stores tag information using an external http service that implements the graphite tags API.
  """
  def __init__(self):
    self.base_url = settings.TAGDB_HTTP_URL
    self.username = settings.TAGDB_HTTP_USER
    self.password = settings.TAGDB_HTTP_PASSWORD

  def request(self, method, url, fields=None, requestContext=None):
    if not fields:
      fields = {}

    headers = requestContext.get('forwardHeaders') if requestContext else {}
    if 'Authorization' not in headers and self.username and self.password:
      headers['Authorization'] = 'Basic ' + ('%s:%s' % (self.username, self.password)).encode('base64')

    result = http.request(
      method,
      self.base_url + url,
      fields={field: value for (field, value) in fields.items() if value is not None},
      headers=headers,
      timeout=settings.REMOTE_FIND_TIMEOUT,
    )

    if result.status != 200:
      raise Exception('HTTP Error from remote tagdb: %s' % result.status)

    return json.loads(result.data.decode('utf-8'))

  def _find_series(self, tags, requestContext=None):
    return self.request(
      'GET',
      '/tags/findSeries?' + '&'.join([('expr=%s' % quote(tag)) for tag in tags]),
      requestContext=requestContext,
    )

  def get_series(self, path, requestContext=None):
    parsed = self.parse(path)

    seriesList = self.find_series(
      [('%s=%s' % (tag, parsed.tags[tag])) for tag in parsed.tags],
      requestContext=requestContext,
    )

    if parsed.path in seriesList:
      return parsed

  def list_tags(self, tagFilter=None, requestContext=None):
    return self.request('GET', '/tags', {'filter': tagFilter}, requestContext)

  def get_tag(self, tag, valueFilter=None, requestContext=None):
    return self.request('GET', '/tags/' + tag, {'filter': valueFilter}, requestContext)

  def list_values(self, tag, valueFilter=None, requestContext=None):
    tagInfo = self.get_tag(tag, valueFilter, requestContext)
    if not tagInfo:
      return []

    return tagInfo['values']

  def tag_series(self, series, requestContext=None):
    return self.request('POST', '/tags/tagSeries', {'path': series}, requestContext)

  def del_series(self, series, requestContext=None):
    return self.request('POST', '/tags/delSeries', {'path': series}, requestContext)
