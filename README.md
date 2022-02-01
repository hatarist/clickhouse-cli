# clickhouse-cli

An unofficial command-line client for the [ClickHouse](https://clickhouse.yandex/) DBMS.
It implements some common and awesome things, such as:

  - Autocompletion (work in progress)
  - Syntax highlighting for the queries & data output (Pretty* formats)
  - Multiquery & multiline modes by default - paste anything as much as you want!
  - Pager support (`less`) for the data output
  - Custom, PostgreSQL-like commands like `\d+ table_name` or `\ps`. See `\?`
  - [User-defined functions](#user-defined-functions)

But it works over the HTTP port, so there are some limitations for now:

  - Doesn't fully support sessions. `SET` options are stored locally and are sent with every request.

## Install

Python 3.4+ is required.

    $ pip3 install clickhouse-cli


## Options

    $ clickhouse-cli --help
    Usage: clickhouse-cli [OPTIONS] [SQLFILE]

      A third-party client for the ClickHouse DBMS.

    Options:
      -h, --host TEXT          Server host (hostname, or URL)
      -p, --port INTEGER       Server HTTP port
      -u, --user TEXT          User
      -P, --password           Password
      -d, --database TEXT      Database
      -s, --settings TEXT      Query string to be appended to every query
      -c, --cookie TEXT        Cookie header to be sent with every query
      -f, --format TEXT        Data format for the interactive mode
      -F, --format-stdin TEXT  Data format for stdin/file queries
      -m, --multiline          Enable multiline shell
      --stacktrace             Print stacktraces received from the server.
      --version                Show the version and exit.
      --help                   Show this message and exit.


## Configuration file

`~/.clickhouse-cli.rc` is here for your service!

    [defaults]
    # Default connection options that will be used if the relevant argument was omitted.

    host = 127.0.0.1
    port = 8123
    db = default
    user = default
    password =
    cookie =

    # It's not secure to store the password here in plain text.


    [main]
    # Disable multiline mode by default
    multiline = False

    # Show SQL statements timing
    timing = True

    # Preferred data format for the interactive mode
    format = PrettyCompact

    # Preferred data format for the non-interactive mode (file/stdin)
    format_stdin = TabSeparated

    # Show the reformatted query after its execution
    show_formatted_query = True

    # Syntax highlighting
    highlight = True

    # Syntax highlight certain output in the interactive mode:
    highlight_output = True

    # Syntax highlighting in TrueColor (if supported, see https://gist.github.com/XVilka/8346728)
    highlight_truecolor = True

    # Pygments Highlight theme (check out https://help.farbox.com/pygments.html for available themes)
    highlight_theme = default

    # Show the output via pager (if defined)
    pager = False


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

    Query suffixes:
    ---------------
    \g, \G  Use the Vertical format.
    \p      Enable the pager.

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

### Environment variables

The available environment variables are:

 - `CLICKHOUSE_HOST`
 - `CLICKHOUSE_PORT`
 - `CLICKHOUSE_USER`
 - `CLICKHOUSE_PASSWORD`
 - `CLICKHOUSE_DATABASE`
 - `CLICKHOUSE_COOKIE`

The order of precedence is:

 - command argument
 - environment variable
 - default value in the `~/.clickhouse-cli.rc`

### Reading from file / stdin

    $ echo 'SELECT 1, 2, 3; SELECT 4, 5, 6;' | clickhouse-cli
    1	2	3

    4	5	6

    $ cat test.sql
    SELECT 1, 2, 3;
    SELECT 4, 5, 6;

    $ clickhouse-cli test.sql
    1 2 3

    4 5 6

    $ clickhouse-cli -F CSV <<< 'SELECT 1, 2, 3 UNION ALL SELECT 4, 5, 6'
    1,2,3
    4,5,6

### Inserting the data from file

    $ clickhouse-cli -q 'CREATE TABLE test (date Date, s String, i UInt64) ENGINE = TinyLog'

    $ cat data.csv
    2017-01-01,hello,1
    2017-02-02,world,2

    $ clickhouse-cli -q 'INSERT INTO test (date, s, i)' -F CSV data.csv

    Ok. Elapsed: 0.037 sec.

    $ clickhouse-cli -q 'SELECT * FROM test'
    2017-01-01	hello	1
    2017-02-02	world	2

### Custom settings

    $ clickhouse-cli -h 10.1.1.14 -s 'max_memory_usage=20000000000&enable_http_compression=1'

### User-defined functions

Oh boy. It's a **very dirty** (and **very untested**) hack that lets you define your own functions or, actually, whatever you want,
by running a find & replace operation over the query before sending the query to the server.

Say, you often run queries that parse some JSON, so you use `visitParamExtractString` all the time:

     :) SELECT date, ip, visitParamExtractString(headers, 'User-Agent') AS ua FROM visits LIMIT 1;

Even with autocompletion, this makes it harder to work with such queries.
With this feature, you'll be able to create custom find & replace pairs to make things a little bit easier (or harder; it depends).
Put this in your `.clickhouse-cli.rc`:

    udf = {
            r'header\((.*?)\)': r'visitParamExtractString(headers, \1)',
      }

And rejoice!

     :) SELECT date, ip, header('User-Agent') AS ua FROM visits LIMIT 1;

The client will replace the matching expressions with another ones, and the query will execute correctly.
See [.clickhouse-cli.rc](https://github.com/hatarist/clickhouse-cli/blob/master/clickhouse_cli/clickhouse-cli.rc.sample) for a full example.
