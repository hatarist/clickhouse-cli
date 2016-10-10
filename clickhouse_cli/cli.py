import click
from prompt_toolkit import Application, CommandLineInterface
from prompt_toolkit.shortcuts import create_eventloop, create_prompt_layout

from clickhouse_cli import __version__
from clickhouse_cli.clickhouse.client import Client, ConnectionError, DBException, TimeoutError
from clickhouse_cli.clickhouse.definitions import EXIT_COMMANDS
from clickhouse_cli.ui.lexer import CHLexer
from clickhouse_cli.ui.prompt import CLIBuffer, KeyBinder, get_continuation_tokens, get_prompt_tokens, query_is_finished
from clickhouse_cli.ui.style import CHStyle, echo


def show_version():
    print("clickhouse-cli version: {version}".format(version=__version__))


class CLI:

    def __init__(self, host, port, user, password, database, fmt, multiline, stacktrace):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.format = fmt
        self.multiline = multiline
        self.stacktrace = stacktrace
        self.url = 'http://{host}:{port}/'.format(host=host, port=port)
        self.client = Client(self.url, self.user, self.password, self.database)

    def connect(self):
        print("Connecting to {host}:{port}".format(host=self.host, port=self.port))

        try:
            response = self.client.query('SELECT 1', fmt='TabSeparated', timeout=10)
        except TimeoutError:
            echo.error("Error: Connection timeout.")
            return False
        except ConnectionError:
            echo.error("Error: Failed to connect.")
            return False
        except DBException as e:
            echo.error("Error:")
            echo.error(e.error)
            return False

        if response.data != '1\n':
            echo.error("Error: Request failed: `SELECT 1` query failed.")
            return False

        echo.success("Connected to ClickHouse server.\n")
        return True

    def run(self, data=None):
        if data is not None:
            # Run in a non-interactive mode
            return self.handle_input(data)

        show_version()
        self.connect()

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
                self.handle_input(cli_input.text.split('\n'))

        except EOFError:
            echo.success("Bye.")

    def handle_input(self, input_data):
        # FIXME: A dirty dirty hack to make multiple queries (per one paste) work.
        query_buffer = ''
        query_wasnt_finished = False

        for query in input_data:
            if not query_is_finished(query):
                query_buffer = query_buffer + ' ' + query
                query_wasnt_finished = True
            else:
                if query_wasnt_finished:
                    self.handle_query(query_buffer + ' ' + query)
                    query_wasnt_finished = False
                    query_buffer = ''
                else:
                    self.handle_query(query)

        if not self.multiline and query_buffer != '':
            self.handle_query(query_buffer)

    def handle_query(self, query):
        if query == '':
            pass
        elif query in EXIT_COMMANDS:
            raise EOFError
        elif query == 'help':
            print_help()
        else:
            response = ''

            try:
                response = self.client.query(query, fmt=self.format)
                print()
            except DBException as e:
                echo.error("\nReceived exception from server:")
                echo.error(e.error)

                if self.stacktrace and e.stacktrace:
                    print("\nStack trace:")
                    print(e.stacktrace)
                return

            if response.data != '':
                print(response.data)

            echo.success('Ok. ', nl=False)

            if response.rows is not None:
                print('{rows_count} row{rows_plural} in set.'.format(
                    rows_count=response.rows,
                    rows_plural='s' if response.rows != 1 else '',
                ), end=' ')

            if response.time_elapsed:
                print('Elapsed: {elapsed:.3f} sec.'.format(
                    elapsed=response.time_elapsed
                ))

            print('', flush=True)


def print_help():
    rows = [
        ['USE db', "Change the current database to `db`."],
        ['QUIT', "Exit clickhouse-cli."],
        ['HELP', "Show this help message."],
    ]

    for row in rows:
        echo.success('{:<8s}'.format(row[0]), nl=False)
        echo.info(row[1])


@click.command()
@click.option('--host', '-h', default='localhost', help="Server host")
@click.option('--port', '-p', default='8123', type=click.INT, help="Server HTTP port")
@click.option('--user', '-u', default='default', help="User")
@click.option('--password', '-P', is_flag=True, help="Password")
@click.option('--database', '-d', default='default', help="Database")
@click.option('--format', '-f', default='PrettyCompactMonoBlock', help="Output format")
@click.option('--multiline', '-m', is_flag=True, help="Enable multiline shell")
@click.option('--stacktrace', is_flag=True, help="Print stacktraces received from the server.")
@click.option('--version', is_flag=True, help="Show the version and exit.")
@click.argument('sqlfile', nargs=1, default=False, type=click.File('r'))
def run(host, port, user, password, database, format, multiline, stacktrace, version, sqlfile):
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
    cli = CLI(host, port, user, password, database, format, multiline, stacktrace)
    cli.run(data=sql_input)


if __name__ == '__main__':
    run()
