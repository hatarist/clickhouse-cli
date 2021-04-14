""" Description: creating the exceptions """
import re

class DBException(Exception):
    """ Description: creating the Custom DB Exception
        params response : passing the response
        params query : passing the query"""

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
        except Exception:
            self.error = self.response.text

    def __str__(self):
        return 'Query:\n{0}\n\nResponse:\n{1}'.format(self.query, self.response.text)


class TimeoutError(Exception):
    """ Description: creating the custom TimeoutError """
    pass

class ConnectionError(Exception):
    """ Description: creating the custom ConnectionError """
    pass
