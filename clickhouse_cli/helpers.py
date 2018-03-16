import email.parser
import http.client


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def numberunit_fmt(num):
    if not num:
        return "0"

    for unit in ['', 'thousand', 'million', 'billion', 'trillion']:
        if abs(num) < 1000.0:
            return ("%3.1f %s" % (num, unit)).strip()
        num /= 1000.0
    return "%.1f %s" % (num, 'quadrillion')


def trace_headers_stream(*args):
    pass


def parse_headers_stream(fp, _class=http.client.HTTPMessage):
    """A modified version of http.client.parse_headers."""
    headers = []

    while True:
        line = fp.readline(http.client._MAXLINE + 1)
        if len(line) > http.client._MAXLINE:
            raise http.client.LineTooLong("header line")

        if line.startswith(b'X-ClickHouse-Progress: ') and trace_headers_stream:
            trace_headers_stream(line)
        else:
            headers.append(line)

        # _MAXHEADERS check was removed here since ClickHouse may send a lot of Progress headers.

        if line in (b'\r\n', b'\n', b''):
            break
    hstring = b''.join(headers).decode('iso-8859-1')
    return email.parser.Parser(_class=_class).parsestr(hstring)
