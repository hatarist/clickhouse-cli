import logging
import re

import requests
import sqlparse
import pygments

from pygments.formatters import TerminalTrueColorFormatter

from clickhouse_cli.clickhouse.definitions import FORMATTABLE_QUERIES
from clickhouse_cli.ui.style import CHPygmentsStyle
from clickhouse_cli.ui.lexer import CHLexer

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

    def __init__(self, query, fmt, response='', message='', stream=False):
        self.query = query
        self.message = message
        self.format = fmt
        self.stream = stream
        self.time_elapsed = None
        self.rows = None

        if isinstance(response, requests.Response):
            self.time_elapsed = response.elapsed.total_seconds()

            if stream:
                self.data = response.iter_lines()
                self.rows = -1
                return

            self.data = response.text

            lines = self.data.split('\n')

            if self.data == '' or not lines:
                self.rows = 0
            elif fmt.startswith('Pretty'):
                self.rows = sum(1 for line in lines if line.startswith('│'))
            elif fmt in ('TabSeparated', 'CSV'):
                self.rows = len(lines) - 1
            elif fmt in ('TabSeparatedWithNames', 'CSVWithNames'):
                self.rows = len(lines) - 2
            elif fmt in ('TabSeparatedWithNamesAndTypes'):
                self.rows = len(lines) - 3
        else:
            self.data = response


class Client(object):

    def __init__(self, url, user='default', password=None, database='default', settings=None, stacktrace=False):
        self.url = url
        self.user = user
        self.password = password or ''
        self.database = database
        self.settings = settings or {}
        self.stacktrace = stacktrace

    def _query(self, query, extra_params, fmt, stream, data=None, **kwargs):
        params = {'query': query}
        params.update(extra_params)

        response = None
        try:
            response = requests.post(
                self.url, data=data, params=params, auth=(self.user, self.password), stream=stream, **kwargs
            )
        except requests.exceptions.ConnectTimeout:
            raise TimeoutError
        except requests.exceptions.ConnectionError:
            raise ConnectionError

        if response is not None and response.status_code != 200:
            raise DBException(response, query=query)

        return Response(query, fmt, response, stream=stream)

    def test_query(self):
        params = {'database': self.database}
        params.update(self.settings)
        return self._query(
            'SELECT 1',
            params,
            fmt='TabSeparated',
            stream=False,
        )

    def kill_query(self, query_id):
        return self._query(
            'SELECT 1',
            {'replace_running_query': 1, 'query_id': query_id},
            fmt='TabSeparated',
            stream=False,
        )

    def query(self, query, data=None, fmt='PrettyCompactMonoBlock', stream=False, verbose=False, query_id=None, **kwargs):
        query = sqlparse.format(query, strip_comments=True).rstrip(';')

        if verbose and kwargs.pop('show_formatted', False):
            # Highlight & reformat the SQL query
            formatted_query = sqlparse.format(
                query,
                reindent=True,
                indent_width=4,
                # keyword_case='upper'  # works poorly in a few cases
            )

            print('\n' + pygments.highlight(
                formatted_query,
                CHLexer(),
                TerminalTrueColorFormatter(style=CHPygmentsStyle)
            ))

        # TODO: use sqlparse's parser instead
        query_split = query.split()

        if len(query_split) == 0:
            return Response(query, fmt)

        # Since sessions aren't supported over HTTP, we have to make some quirks:
        # USE database;
        if query_split[0].upper() == 'USE' and len(query_split) == 2:
            old_database = self.database
            self.database = query_split[1]
            try:
                self.test_query()
            except DBException as e:
                self.database = old_database
                raise e

            return Response(query, fmt, message='Changed the current database to {0}.'.format(self.database))

        # SET foo = bar;
        if query_split[0].upper() == 'SET' and len(query_split) == 4:
            key, value = query_split[1], query_split[3]
            old_value = self.settings.get(key)
            self.settings[key] = value
            try:
                self.test_query()
            except DBException as e:
                if old_value is not None:
                    self.settings[key] = old_value
                else:
                    del self.settings[key]
                raise e

            return Response(query, fmt, response='', message='Set {0} to {1}.'.format(key, value))

        if query_split[0].upper() in FORMATTABLE_QUERIES and len(query_split) >= 2:
            if query_split[-2].upper() == 'FORMAT':
                fmt = query_split[-1]
            elif query_split[-2].upper() != 'FORMAT':
                if query_split[0].upper() != 'INSERT' or data is not None:
                    query = query + ' FORMAT {fmt}'.format(fmt=fmt)

        params = {'database': self.database, 'stacktrace': int(self.stacktrace)}
        if query_id:
            params['query_id'] = query_id

        params.update(self.settings)
        return self._query(query, params, fmt=fmt, stream=stream, **kwargs)
