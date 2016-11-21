from urllib.parse import parse_qs

import click
import sqlparse
from prompt_toolkit import Application, CommandLineInterface
from prompt_toolkit.shortcuts import create_eventloop, create_prompt_layout

from clickhouse_cli import __version__
from clickhouse_cli.clickhouse.client import Client, ConnectionError, DBException, TimeoutError
from clickhouse_cli.clickhouse.definitions import EXIT_COMMANDS
from clickhouse_cli.clickhouse.sqlparse_patch import KEYWORDS
from clickhouse_cli.ui.lexer import CHLexer
from clickhouse_cli.ui.prompt import (
    CLIBuffer,
    KeyBinder,
    get_continuation_tokens,
    get_prompt_tokens,
    query_is_finished,
)
from clickhouse_cli.ui.style import CHStyle, Echo
from clickhouse_cli.config import read_config

# monkey-patch sqlparse
sqlparse.keywords.KEYWORDS = KEYWORDS
sqlparse.keywords.KEYWORDS_COMMON = {}
sqlparse.keywords.KEYWORDS_ORACLE = {}


def show_version():
    print("clickhouse-cli version: {version}".format(version=__version__))


class CLI:

    def __init__(self, host, port, user, password, database, settings, format, format_stdin, multiline, stacktrace):
        self.config = None

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.settings = {k: v[0] for k, v in parse_qs(settings).items()}
        self.format = format
        self.format_stdin = format_stdin
        self.multiline = multiline
        self.stacktrace = stacktrace

        self.echo = Echo(verbose=True)

        self.url = 'http://{host}:{port}/'.format(host=host, port=port)
        self.client = Client(self.url, self.user, self.password, self.database, self.settings, self.stacktrace)

    def connect(self):
        print("Connecting to {host}:{port}".format(host=self.host, port=self.port))

        try:
            response = self.client.query('SELECT 1', fmt='TabSeparated', timeout=10)
        except TimeoutError:
            self.echo.error("Error: Connection timeout.")
            return False
        except ConnectionError:
            self.echo.error("Error: Failed to connect.")
            return False
        except DBException as e:
            self.echo.error("Error:")
            self.echo.error(e.error)

            if self.stacktrace and e.stacktrace:
                self.echo.print("Stack trace:")
                self.echo.print(e.stacktrace)

            return False

        if response.data != '1\n':
            self.echo.error("Error: Request failed: `SELECT 1` query failed.")
            return False

        self.echo.success("Connected to ClickHouse server.\n")
        return True

    def load_config(self):
        self.config = read_config()

        if self.config.getboolean('main', 'multiline'):
            self.multiline = True

        self.format = self.format or self.config.get('main', 'format', fallback='PrettyCompactMonoBlock')
        self.format_stdin = self.format_stdin or self.config.get('main', 'format_stdin', fallback='TabSeparated')

    def run(self, query=None, data=None):
        self.load_config()

        if data is not None:
            self.format = self.format_stdin

            # Run in a non-interactive mode
            if query is None:
                # Run stdin/file as SQL query
                self.echo.verbose = False
                return self.handle_input(data)

        if query is not None:
            self.format = self.format_stdin

            if data is None:
                self.echo.verbose = False
                return self.handle_query(query, stream=True)
            else:
                return self.handle_query(query, data=data, stream=True)

        show_version()
        if not self.connect():
            return

        layout = create_prompt_layout(
            lexer=CHLexer,
            get_prompt_tokens=get_prompt_tokens,
            get_continuation_tokens=get_continuation_tokens,
            multiline=self.multiline,
        )

        buffer = CLIBuffer(
            client=self.client,
            multiline=self.multiline,
        )

        application = Application(
            layout=layout,
            buffer=buffer,
            style=CHStyle,
            key_bindings_registry=KeyBinder.registry,
        )

        eventloop = create_eventloop()

        cli = CommandLineInterface(application=application, eventloop=eventloop)

        try:
            while True:
                cli_input = cli.run(reset_current_buffer=True)
                self.handle_input(cli_input.text)
        except EOFError:
            self.echo.success("Bye.")

    def handle_input(self, input_data):
        # FIXME: A dirty dirty hack to make multiple queries (per one paste) work.

        for query in sqlparse.split(input_data):
            self.handle_query(query)

    def handle_query(self, query, data=None, stream=False):
        if query == '':
            return
        elif query in EXIT_COMMANDS:
            raise EOFError
        elif query in ('\?', 'help'):
            rows = [
                ['', ''],
                ["clickhouse-cli's custom commands:", ""],
                ['---------------------------------', ''],
                ['USE db', "Change the current database to `db`."],
                ['QUIT', "Exit clickhouse-cli."],
                ['HELP', "Show this help message."],
                ['', ""],
                ["PostgreSQL-like custom commands:", ""],
                ['--------------------------------', ''],
                ['\l', "Show databases."],
                ['\c', "Change the current database."],
                ['\d, \dt', "Show tables in the current database."],
                ['\d+', "Show table's schema."],
                ['', ''],
            ]

            for row in rows:
                self.echo.success('{:<8s}'.format(row[0]), nl=False)
                self.echo.info(row[1])
            return

        elif query in ('\d', '\dt'):
            query = 'SHOW TABLES'
        elif query in ('\l',):
            query = 'SHOW DATABASES'
        elif query.startswith('\d+ '):
            query = 'DESCRIBE TABLE ' + query[4:]
        elif query.startswith('\c '):
            query = 'USE ' + query[3:]

        response = ''

        try:
            response = self.client.query(query, fmt=self.format, data=data, stream=stream)
        except DBException as e:
            self.echo.error("\nReceived exception from server:")
            self.echo.error(e.error)

            if self.stacktrace and e.stacktrace:
                self.echo.print("\nStack trace:")
                self.echo.print(e.stacktrace)

                self.echo.print('Elapsed: {elapsed:.3f} sec.\n'.format(
                    elapsed=e.response.elapsed.total_seconds()
                ))

            return

        self.echo.print()

        if stream:
            print('\n'.join(response.data.decode('utf-8', 'ignore')), end='')
        else:
            print(response.data, end='')

        if response.message != '':
            self.echo.print()
            self.echo.print(response.message)

        self.echo.success('\nOk. ', nl=False)

        if response.rows is not None:
            self.echo.print('{rows_count} row{rows_plural} in set.'.format(
                rows_count=response.rows,
                rows_plural='s' if response.rows != 1 else '',
            ), end=' ')

        if self.config.getboolean('main', 'timing') and response.time_elapsed is not None:
            self.echo.print('Elapsed: {elapsed:.3f} sec.'.format(
                elapsed=response.time_elapsed
            ), end='')

        self.echo.print('\n')


@click.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.option('--host', '-h', default='localhost', help="Server host")
@click.option('--port', '-p', default='8123', type=click.INT, help="Server HTTP port")
@click.option('--user', '-u', default='default', help="User")
@click.option('--password', '-P', is_flag=True, help="Password")
@click.option('--database', '-d', default='default', help="Database")
@click.option('--settings', '-s', help="Query string to be sent with every query")
@click.option('--query', '-q', help="Query to execute")
@click.option('--format', '-f', help="Output format for the interactive mode")
@click.option('--format-stdin', '-F', help="Output format for stdin/file queries")
@click.option('--multiline', '-m', is_flag=True, help="Enable multiline shell")
@click.option('--stacktrace', is_flag=True, help="Print stacktraces received from the server.")
@click.option('--version', is_flag=True, help="Show the version and exit.")
@click.argument('sqlfile', nargs=1, default=False, type=click.File('r'))
def run_cli(host, port, user, password, database, settings, query, format, format_stdin, multiline, stacktrace, version, sqlfile):
    """
    A third-party client for the ClickHouse DBMS.
    """
    if version:
        return show_version()

    if user and password:
        password = click.prompt("Password", hide_input=True, show_default=False, type=str)

    sql_input = None

    # Read from STDIN if non-interactive mode
    stdin = click.get_text_stream('stdin')
    if not stdin.isatty():
        sql_input = stdin.readlines()

    # Read the given file
    if sqlfile.name is not False:
        sql_input = sqlfile.readlines()

    # TODO: Rename the CLI's instance into something more feasible
    cli = CLI(host, port, user, password, database, settings, format, format_stdin, multiline, stacktrace)
    cli.run(query=query, data=sql_input)


if __name__ == '__main__':
    run_cli()
