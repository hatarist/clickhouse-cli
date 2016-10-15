import logging
import re

import requests

from .definitions import FORMATTABLE_QUERIES


logger = logging.getLogger('main')


class DBException(Exception):
    regex = (
        r'Code: (?P<code>\d+), e\.displayText\(\) = ([\w:]+: )?(?P<text>[\w\W]+),\s+'
        r'e\.what\(\) = (?P<what>[\w:]+)(,\s+)?'
        r'(Stack trace:\n\n(?P<stacktrace>[\w\W]*)\n)?'
    )

    def __init__(self, response, query):
        self.response = response
        self.query = query
        self.error_code = 0
        self.error = ''
        self.stacktrace = ''

        try:
            info = re.search(self.regex, response.text).groupdict()
            self.error_code = info['code']
            self.error = info['text']
            self.stacktrace = info['stacktrace'] or ''
        except:
            self.error = self.response.text

    def __str__(self):
        return 'Query:\n{0}\n\nResponse:\n{1}'.format(self.query, self.response.text)


class TimeoutError(Exception):
    pass


class ConnectionError(Exception):
    pass


class Response(object):

    def __init__(self, query, fmt, response='', message=''):
        self.query = query
        self.message = message
        self.format = fmt
        self.time_elapsed = None
        self.rows = None

        if isinstance(response, requests.Response):
            self.data = response.text[:-1]
            self.time_elapsed = response.elapsed.total_seconds()

            lines = len(self.data.split('\n'))

            if self.data == '' or not lines:
                self.rows = 0
            elif fmt in ('TabSeparated', 'CSV'):
                self.rows = lines
            elif fmt in ('TabSeparatedWithNames', ):
                self.rows = lines - 1
            elif fmt in ('PrettyCompactMonoBlock', 'TabSeparatedWithNamesAndTypes'):
                self.rows = lines - 2

            if fmt in ('PrettyCompactMonoBlock',) and self.rows >= 10001:
                self.rows = 10000
        else:
            self.data = response


class Client(object):

    def __init__(self, url, user='default', password=None, database='default', stacktrace=False):
        self.url = url
        self.user = user
        self.password = password or ''
        self.database = database
        self.stacktrace = stacktrace

    def query(self, query, data=None, fmt='PrettyCompactMonoBlock', **kwargs):
        query = query.strip().rstrip(';').rstrip()

        query_split = query.split()

        if len(query_split) == 0:
            return Response(query, fmt, message='Empty query.'.format(self.database))

        # A `USE database;` kind of query that we should handle ourselves since sessions aren't supported over HTTP
        if query_split[0].upper() == 'USE' and len(query_split) == 2:
            self.database = query_split[1]
            return Response(query, fmt, message='Changed the current database to {0}.'.format(self.database))

        if query_split[0].upper() in FORMATTABLE_QUERIES and len(query_split) >= 2:
            if query_split[-2].upper() == 'FORMAT':
                fmt = query_split[-1]
            elif query_split[-2].upper() != 'FORMAT':
                if query_split[0].upper() != 'INSERT':
                    query = query + ' FORMAT {fmt}'.format(fmt=fmt)

        params = {'query': query}

        if self.database != 'default':
            params['database'] = self.database

        if self.stacktrace:
            params['stacktrace'] = 1

        response = None
        try:
            response = requests.post(self.url, data=data, params=params, auth=(self.user, self.password), **kwargs)
        except requests.exceptions.ConnectTimeout:
            raise TimeoutError
        except requests.exceptions.ConnectionError:
            raise ConnectionError

        if response is not None and response.status_code != 200:
            raise DBException(response, query=query)

        return Response(query, fmt, response)
