import email.parser
import http.client
import io


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


def chain_streams(streams, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """
    https://stackoverflow.com/questions/24528278/stream-multiple-files-into-a-readable-object-in-python
    
    Chain an iterable of streams together into a single buffered stream.
    Usage:
        def generate_open_file_streams():
            for file in filenames:
                yield open(file, 'rb')
        f = chain_streams(generate_open_file_streams())
        f.read()
    """

    class ChainStream(io.RawIOBase):
        def __init__(self):
            self.leftover = b''
            self.stream_iter = iter(streams)
            try:
                self.stream = next(self.stream_iter)
            except StopIteration:
                self.stream = None

        def readable(self):
            return True

        def _read_next_chunk(self, max_length):
            # Return 0 or more bytes from the current stream, first returning all
            # leftover bytes. If the stream is closed returns b''
            if self.leftover:
                return self.leftover
            elif self.stream is not None:
                return self.stream.read(max_length)
            else:
                return b''

        def readinto(self, b):
            buffer_length = len(b)
            chunk = self._read_next_chunk(buffer_length)
            while len(chunk) == 0:
                # move to next stream
                if self.stream is not None:
                    self.stream.close()
                try:
                    self.stream = next(self.stream_iter)
                    chunk = self._read_next_chunk(buffer_length)
                except StopIteration:
                    # No more streams to chain together
                    self.stream = None
                    return 0  # indicate EOF
            output, self.leftover = chunk[:buffer_length], chunk[buffer_length:]
            b[:len(output)] = output
            return len(output)

    return io.BufferedReader(ChainStream(), buffer_size=buffer_size)