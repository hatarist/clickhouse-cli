# clickhouse-cli

An unofficial command-line client for the [ClickHouse](https://clickhouse.yandex/) DBMS.  
It works over the HTTP port, so there are limitations:

  - Doesn't fully support sessions
  - Doesn't show progress bar and memory usage stats

But, though, it has:

  - Autocompletion (WIP)
  - Syntax highlighting: queries, output (Pretty*)
  - Multiquery mode by default
  - Allows you to paste multiline queries by default
  - Session emulation: `SET` is stored and processed locally
  - Query termination by Ctrl+C (using `replace_running_query`)

  - `\ps` (a shorter version of `SHOW PROCESSLIST`) and `\kill <query-id>` (that aborts the given query)


## Install

Python 3.4+ is required.

    $ pip3 install clickhouse-cli


## Options

    $ clickhouse-cli --help
    Usage: clickhouse-cli [OPTIONS] [SQLFILE]
    
      A third-party client for the ClickHouse DBMS.
    
    Options:
      -h, --host TEXT          Server host
      -p, --port INTEGER       Server HTTP port
      -u, --user TEXT          User
      -P, --password           Password
      -d, --database TEXT      Database
      -s, --settings TEXT      Query string to be appended to every query
      -f, --format TEXT        Output format for the interactive mode
      -F, --format-stdin TEXT  Output format for stdin/file queries
      -m, --multiline          Enable multiline shell
      --stacktrace             Print stacktraces received from the server.
      --version                Show the version and exit.
      --help                   Show this message and exit.


## Configuration file

`~/.clickhouse-cli.rc` is here for your service!

    [main]
    # Enable multiline mode by default
    multiline = False

    # Show SQL statements timing
    timing = True

    # Preferred output format for the interactive mode
    format = PrettyCompactMonoBlock

    # Preferred output format for the non-interactive mode (file/stdin)
    format_stdin = TabSeparated

    # Show the reformatted query after its execution
    show_formatted_query = True

    # Syntax highlight certain output in the interactive mode:
    highlight_output = True

    [settings]
    # You can place the server-side settings here!
    # max_memory_usage = 20000000000


## Quickstart

    $ clickhouse-cli
    clickhouse-cli version: 0.1.6
    Connecting to localhost:8123
    Connected to ClickHouse server.

     :) help

    clickhouse-cli's custom commands:
    ---------------------------------
    USE     Change the current database.
    SET     Set an option for the current CLI session.
    QUIT    Exit clickhouse-cli.
    HELP    Show this help message.

    PostgreSQL-like custom commands:
    --------------------------------
    \l      Show databases.
    \c      Change the current database.
    \d, \dt Show tables in the current database.
    \d+     Show table's schema.
    \ps     Show current queries.
    \kill   Kill query by its ID.

     :) \l

    ┌─name───────┐
    │ default    │
    │ quickstart │
    │ system     │
    └────────────┘

    Ok. 3 rows in set. Elapsed: 0.022 sec.

     :) USE quickstart

    Changed the current database to quickstart.

    Ok.

     :) \dt

    ┌─name───┐
    │ ontime │
    └────────┘

    Ok. 1 row in set. Elapsed: 0.012 sec.

     :) SELECT OriginCityName, count(*) AS flights
        FROM ontime GROUP BY OriginCityName ORDER BY flights DESC LIMIT 5

    ┌─OriginCityName────────┬──flights─┐
    │ Chicago, IL           │ 10536203 │
    │ Atlanta, GA           │  8867847 │
    │ Dallas/Fort Worth, TX │  7601863 │
    │ Houston, TX           │  5714988 │
    │ Los Angeles, CA       │  5575119 │
    └───────────────────────┴──────────┘

    Ok. 5 rows in set. Elapsed: 1.317 sec.


## Advanced usage

### Reading from file / stdin

    $ echo 'select 1, 2, 3; select 4, 5, 6;' | clickhouse-cli
    1	2	3
    4	5	6

    $ clickhouse-cli test.sql
    1 2 3
    4 5 6

    $ clickhouse-cli -F CSV <<< 'select 1, 2, 3; select 4, 5, 6'
    1,2,3
    4,5,6


### Custom settings

    $ clickhouse-cli -h 10.1.1.14 -s 'max_memory_usage=20000000000&enable_http_compression=1'
