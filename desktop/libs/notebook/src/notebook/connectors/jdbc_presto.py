#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import logging

from librdbms.jdbc import query_and_fetch
from notebook.connectors.jdbc import JdbcApi
from notebook.connectors.jdbc import Assist
from notebook.connectors.jdbc import query_error_handler
from desktop.lib import fsmanager
from desktop.lib import export_csvxls
from desktop.lib.paths import SAFE_CHARACTERS_URI_COMPONENTS
from desktop.lib.exceptions_renderable import PopupException
from beeswax.conf import DOWNLOAD_ROW_LIMIT, DOWNLOAD_BYTES_LIMIT
from useradmin.models import User
from desktop.auth.backend import rewrite_user
from django.urls import reverse
from retry_decorator import *

if sys.version_info[0] > 2:
  from urllib.parse import quote as urllib_quote
else:
  from urllib import quote as urllib_quote

LOG = logging.getLogger(__name__)

class JdbcApiPresto(JdbcApi):

  def _createAssist(self, db):
    return PrestoAssist(db)

  def _add_limit(self, query):
    max_rows = DOWNLOAD_ROW_LIMIT.get()
    if 'select' in query.strip().lower() and 'limit' not in query.strip().lower():
        return "{} limit {}".format(query, max_rows)

    return query

  def download(self, notebook, snippet, file_format='csv'):
    max_rows = DOWNLOAD_ROW_LIMIT.get()
    max_bytes = DOWNLOAD_BYTES_LIMIT.get()

    query = self._add_limit(snippet['statement'])
    content_generator = PrestoDataAdapter(self.db, query, max_rows=max_rows, start_over=True, max_bytes=max_bytes)
    return export_csvxls.create_generator(content_generator, file_format)

  def upload(self, path, user, content_generator, fs):
    """
    upload(query_model, path, user, db, fs) -> None

    Retrieve the query result in the format specified and upload to hdfs.
    """
    if fs.do_as_user(user.username, fs.exists, path):
        raise Exception(("%s already exists.") % path)
    else:
        fs.do_as_user(user.username, fs.create, path)

    for header, data in content_generator:
        dataset = export_csvxls.dataset(header, data)
        fs.do_as_user(user.username, fs.append, path, dataset.csv)

  @query_error_handler
  def export_data_as_hdfs_file(self, snippet, target_file, overwrite):
    max_rows = DOWNLOAD_ROW_LIMIT.get()
    max_bytes = DOWNLOAD_BYTES_LIMIT.get()
    fs = fsmanager.get_filesystem('default')
    user = User.objects.get(username=self.db.username)
    user = rewrite_user(user)
    fs.setuser(user)

    query = self._add_limit(snippet['statement'])
    content_generator = PrestoDataAdapter(self.db, query, max_rows=max_rows, start_over=True, max_bytes=max_bytes)
    self.upload(target_file, user, content_generator, fs)

    return '/filebrowser/view=%s' % urllib_quote(
        urllib_quote(target_file.encode('utf-8'), safe=SAFE_CHARACTERS_URI_COMPONENTS)
    ) # Quote twice, because of issue in the routing on client

  def export_data_as_table(self, notebook, snippet, destination, is_temporary=False, location=None):
    query = snippet['statement']

    if 'select' not in query.strip().lower():
      raise PopupException(('Only SELECT statements can be saved. Provided statement: %(query)s') % {'query': query})

    database = 'tmp'
    table = destination

    if '.' in table:
      database, table = table.split('.', 1)

    sql = 'CREATE %s TABLE %s.%s %s AS %s' % (
        'TEMPORARY' if is_temporary else '', database, table, "LOCATION '%s'" % location if location else '', query
    )
    success_url = reverse('metastore:describe_table', kwargs={'database': database, 'table': table})

    return sql, success_url

class PrestoAssist(Assist):

  def get_tables_full(self, database, table_names=[]):
    tables, description = query_and_fetch(self.db, "SELECT TABLE_NAME, null AS TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s'" % database)
    return [{"comment": table[1] and table[1].strip(), "type": "Table", "name": table[0] and table[0].strip()} for table in tables]

  def get_columns_full(self, database, table):
    columns, description = query_and_fetch(self.db, "SELECT COLUMN_NAME, DATA_TYPE, COMMENT AS COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'" % (database, table))
    return [{"comment": col[2] and col[2].strip(), "type": col[1], "name": col[0] and col[0].strip()} for col in columns]

  @retry(Exception, tries = 3)
  def get_sample_data(self, database, table, column=None):
    column = column or '*'
    return query_and_fetch(self.db, 'SELECT %s FROM %s.%s limit 100' % (column, database, table))

def PrestoDataAdapter(jdbc, statement, max_rows=-1, start_over=True, max_bytes=-1):
  if max_rows == -1:
    data, description = query_and_fetch(jdbc, statement)
  else:
    data, description = query_and_fetch(jdbc, statement, max_rows)

  headers = [str(col[0]) for col in description]

  yield headers, data
