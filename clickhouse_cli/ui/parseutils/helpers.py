import re
from collections import defaultdict, namedtuple

import sqlparse
from sqlparse.sql import Comparison, Identifier, Where
from sqlparse.tokens import Name

from clickhouse_cli.clickhouse.definitions import KEYWORDS
from clickhouse_cli.ui.parseutils.ctes import isolate_query_ctes
from clickhouse_cli.ui.parseutils.tables import TableReference, extract_tables
from clickhouse_cli.ui.parseutils.utils import find_prev_keyword, last_word, parse_partial_identifier

Special = namedtuple("Special", [])
Database = namedtuple("Database", [])
Schema = Database
# FromClauseItem is a table/view/function used in the FROM clause
# `table_refs` contains the list of tables/... already in the statement,
# used to ensure that the alias we suggest is unique
FromClauseItem = namedtuple("FromClauseItem", "schema table_refs local_tables")
Table = namedtuple("Table", ["schema", "table_refs", "local_tables"])
View = namedtuple("View", ["schema", "table_refs"])
# JoinConditions are suggested after ON, e.g. 'foo.barid = bar.barid'
JoinCondition = namedtuple("JoinCondition", ["table_refs", "parent"])
# Joins are suggested after JOIN, e.g. 'foo ON foo.barid = bar.barid'
Join = namedtuple("Join", ["table_refs", "schema"])

Function = namedtuple("Function", ["schema", "table_refs", "filter"])
# For convenience, don't require the `filter` argument in Function constructor
Function.__new__.__defaults__ = (None, tuple(), None)
Table.__new__.__defaults__ = (None, tuple(), tuple())
View.__new__.__defaults__ = (None, tuple())
FromClauseItem.__new__.__defaults__ = (None, tuple(), tuple())

Column = namedtuple("Column", ["table_refs", "require_last_table", "local_tables", "qualifiable"])
Column.__new__.__defaults__ = (None, None, tuple(), False)

Keyword = namedtuple("Keyword", [])
Datatype = namedtuple("Datatype", ["schema"])
Format = namedtuple("Format", [])
Alias = namedtuple("Alias", ["aliases"])

Path = namedtuple("Path", [])


white_space_regex = re.compile("\\s+", re.MULTILINE)


def _compile_regex(keyword):
    # Surround the keyword with word boundaries and replace interior whitespace
    # with whitespace wildcards
    pattern = "\\b" + white_space_regex.sub(r"\\s+", keyword) + "\\b"
    return re.compile(pattern, re.MULTILINE | re.IGNORECASE)


keyword_regexs = dict((kw, _compile_regex(kw)) for kw in KEYWORDS)


class PrevalenceCounter(object):
    def __init__(self):
        self.keyword_counts = defaultdict(int)
        self.name_counts = defaultdict(int)

    def update(self, text):
        self.update_keywords(text)
        self.update_names(text)

    def update_names(self, text):
        for parsed in sqlparse.parse(text):
            for token in parsed.flatten():
                if token.ttype in Name:
                    self.name_counts[token.value] += 1

    def clear_names(self):
        self.name_counts = defaultdict(int)

    def update_keywords(self, text):
        # Count keywords. Can't rely for sqlparse for this, because it's
        # database agnostic
        for keyword, regex in keyword_regexs.items():
            for _ in regex.finditer(text):
                self.keyword_counts[keyword] += 1

    def keyword_count(self, keyword):
        return self.keyword_counts[keyword]

    def name_count(self, name):
        return self.name_counts[name]


class SqlStatement(object):
    def __init__(self, full_text, text_before_cursor):
        self.identifier = None
        self.word_before_cursor = word_before_cursor = last_word(text_before_cursor, include="many_punctuations")

        full_text, text_before_cursor, self.local_tables = isolate_query_ctes(full_text, text_before_cursor)

        self.text_before_cursor_including_last_word = text_before_cursor

        # If we've partially typed a word then word_before_cursor won't be an
        # empty string. In that case we want to remove the partially typed
        # string before sending it to the sqlparser. Otherwise the last token
        # will always be the partially typed string which renders the smart
        # completion useless because it will always return the list of
        # keywords as completion.
        if self.word_before_cursor:
            if word_before_cursor[-1] == "(" or word_before_cursor[0] == "\\":
                parsed = sqlparse.parse(text_before_cursor)
            else:
                text_before_cursor = text_before_cursor[: -len(word_before_cursor)]
                parsed = sqlparse.parse(text_before_cursor)
                self.identifier = parse_partial_identifier(word_before_cursor)
        else:
            parsed = sqlparse.parse(text_before_cursor)

        full_text, text_before_cursor, parsed = _split_multiple_statements(full_text, text_before_cursor, parsed)

        self.full_text = full_text
        self.text_before_cursor = text_before_cursor
        self.parsed = parsed

        self.last_token = parsed and parsed.token_prev(len(parsed.tokens))[1] or ""

    def is_insert(self):
        return self.parsed.token_first().value.lower() == "insert"

    def get_tables(self, scope="full"):
        """Gets the tables available in the statement.
        param `scope:` possible values: 'full', 'insert', 'before'
        If 'insert', only the first table is returned.
        If 'before', only tables before the cursor are returned.
        If not 'insert' and the stmt is an insert, the first table is skipped.
        """
        tables = extract_tables(self.full_text if scope == "full" else self.text_before_cursor)
        if scope == "insert":
            tables = tables[:1]
        elif self.is_insert():
            tables = tables[1:]
        return tables

    def get_identifier_schema(self):
        schema = (self.identifier and self.identifier.get_parent_name()) or None
        # If schema name is unquoted, lower-case it
        if schema and self.identifier.value[0] != '"':
            schema = schema.lower()

        return schema

    def reduce_to_prev_keyword(self, n_skip=0):
        prev_keyword, self.text_before_cursor = find_prev_keyword(self.text_before_cursor, n_skip=n_skip)
        return prev_keyword


def suggest_type(full_text, text_before_cursor):
    """Takes the full_text that is typed so far and also the text before the
    cursor to suggest completion type and scope.

    Returns a tuple with a type of entity ('table', 'column' etc) and a scope.
    A scope for a column category will be a list of tables.
    """

    if full_text.startswith("\\i "):
        return (Path(),)

    # This is a temporary hack; the exception handling
    # here should be removed once sqlparse has been fixed
    try:
        stmt = SqlStatement(full_text, text_before_cursor)
    except (TypeError, AttributeError):
        return []

    # # Check for special commands and handle those separately
    # if stmt.parsed:
    #     # Be careful here because trivial whitespace is parsed as a
    #     # statement, but the statement won't have a first token

    #     tok1 = stmt.parsed.token_first()
    #     if tok1 and tok1.value == '\\':
    #         text = stmt.text_before_cursor + stmt.word_before_cursor
    #         return suggest_special(text)

    return suggest_based_on_last_token(stmt.last_token, stmt)


function_body_pattern = re.compile(r"(\\$.*?\\$)([\s\S]*?)\\1", re.M)


def _find_function_body(text):
    split = function_body_pattern.search(text)
    return (split.start(2), split.end(2)) if split else (None, None)


def _statement_from_function(full_text, text_before_cursor, statement):
    current_pos = len(text_before_cursor)
    body_start, body_end = _find_function_body(full_text)
    if body_start is None:
        return full_text, text_before_cursor, statement
    if not body_start <= current_pos < body_end:
        return full_text, text_before_cursor, statement
    full_text = full_text[body_start:body_end]
    text_before_cursor = text_before_cursor[body_start:]
    parsed = sqlparse.parse(text_before_cursor)
    return _split_multiple_statements(full_text, text_before_cursor, parsed)


def _split_multiple_statements(full_text, text_before_cursor, parsed):
    if len(parsed) > 1:
        # Multiple statements being edited -- isolate the current one by
        # cumulatively summing statement lengths to find the one that bounds
        # the current position
        current_pos = len(text_before_cursor)
        stmt_start, stmt_end = 0, 0

        for statement in parsed:
            stmt_len = len(str(statement))
            stmt_start, stmt_end = stmt_end, stmt_end + stmt_len

            if stmt_end >= current_pos:
                text_before_cursor = full_text[stmt_start:current_pos]
                full_text = full_text[stmt_start:]
                break

    elif parsed:
        # A single statement
        statement = parsed[0]
    else:
        # The empty string
        return full_text, text_before_cursor, None

    token2 = None
    if statement.get_type() in ("CREATE", "CREATE OR REPLACE"):
        token1 = statement.token_first()
        if token1:
            token1_idx = statement.token_index(token1)
            token2 = statement.token_next(token1_idx)[1]
    if token2 and token2.value.upper() == "FUNCTION":
        full_text, text_before_cursor, statement = _statement_from_function(full_text, text_before_cursor, statement)
    return full_text, text_before_cursor, statement


def suggest_based_on_last_token(token, stmt):
    if isinstance(token, str):
        token_v = token.lower()
    elif isinstance(token, Comparison):
        # If 'token' is a Comparison type such as
        # 'select * FROM abc a JOIN def d ON a.id = d.'. Then calling
        # token.value on the comparison type will only return the lhs of the
        # comparison. In this case a.id. So we need to do token.tokens to get
        # both sides of the comparison and pick the last token out of that
        # list.
        token_v = token.tokens[-1].value.lower()
    elif isinstance(token, Where):
        # sqlparse groups all tokens from the where clause into a single token
        # list. This means that token.value may be something like
        # 'where foo > 5 and '. We need to look "inside" token.tokens to handle
        # suggestions in complicated where clauses correctly
        prev_keyword = stmt.reduce_to_prev_keyword()
        return suggest_based_on_last_token(prev_keyword, stmt)
    elif isinstance(token, Identifier):
        # If the previous token is an identifier, we can suggest datatypes if
        # we're in a parenthesized column/field list, e.g.:
        #       CREATE TABLE foo (Identifier <CURSOR>
        #       CREATE FUNCTION foo (Identifier <CURSOR>
        # If we're not in a parenthesized list, the most likely scenario is the
        # user is about to specify an alias, e.g.:
        #       SELECT Identifier <CURSOR>
        #       SELECT foo FROM Identifier <CURSOR>
        prev_keyword, _ = find_prev_keyword(stmt.text_before_cursor)
        if prev_keyword and prev_keyword.value == "(":
            # Suggest datatypes
            return suggest_based_on_last_token("type", stmt)
        else:
            return (Keyword(),)
    else:
        token_v = token.value.lower()

    if not token:
        return (Keyword(), Special())
    elif token_v.endswith("("):
        p = sqlparse.parse(stmt.text_before_cursor)[0]

        if p.tokens and isinstance(p.tokens[-1], Where):
            # Four possibilities:
            #  1 - Parenthesized clause like "WHERE foo AND ("
            #        Suggest columns/functions
            #  2 - Function call like "WHERE foo("
            #        Suggest columns/functions
            #  3 - Subquery expression like "WHERE EXISTS ("
            #        Suggest keywords, in order to do a subquery
            #  4 - Subquery OR array comparison like "WHERE foo = ANY("
            #        Suggest columns/functions AND keywords. (If we wanted to be
            #        really fancy, we could suggest only array-typed columns)

            column_suggestions = suggest_based_on_last_token("where", stmt)

            # Check for a subquery expression (cases 3 & 4)
            where = p.tokens[-1]
            prev_tok = where.token_prev(len(where.tokens) - 1)[1]

            if isinstance(prev_tok, Comparison):
                # e.g. "SELECT foo FROM bar WHERE foo = ANY("
                prev_tok = prev_tok.tokens[-1]

            prev_tok = prev_tok.value.lower()
            if prev_tok == "exists":
                return (Keyword(),)
            else:
                return column_suggestions

        # Get the token before the parens
        prev_tok = p.token_prev(len(p.tokens) - 1)[1]

        if prev_tok and prev_tok.value and prev_tok.value.lower().split(" ")[-1] == "using":
            # tbl1 INNER JOIN tbl2 USING (col1, col2)
            tables = stmt.get_tables("before")
            # suggest columns that are present in more than one table
            return (
                Column(
                    table_refs=tables,
                    require_last_table=True,
                    local_tables=stmt.local_tables,
                ),
            )

        elif p.token_first().value.lower() == "select":
            # If the lparen is preceeded by a space chances are we're about to
            # do a sub-select.
            if last_word(stmt.text_before_cursor, "all_punctuations").startswith("("):
                return (Keyword(),)
        prev_prev_tok = prev_tok and p.token_prev(p.token_index(prev_tok))[1]
        if prev_prev_tok and prev_prev_tok.normalized == "INTO":
            return (Column(table_refs=stmt.get_tables("insert")),)
        # We're probably in a function argument list
        return (
            Column(
                table_refs=extract_tables(stmt.full_text),
                local_tables=stmt.local_tables,
                qualifiable=True,
            ),
        )
    elif token_v == "set":
        return (Column(table_refs=stmt.get_tables(), local_tables=stmt.local_tables),)
    elif token_v in ("select", "where", "having", "by", "distinct"):
        # Check for a table alias or schema qualification
        parent = (stmt.identifier and stmt.identifier.get_parent_name()) or None
        tables = stmt.get_tables()
        if parent:
            tables = tuple(t for t in tables if identifies(parent, t))
            return (
                Column(table_refs=tables, local_tables=stmt.local_tables),
                Table(schema=parent),
                View(schema=parent),
                Function(schema=parent),
            )
        else:
            tables = tuple(
                TableReference(
                    schema=table.schema or "default",
                    name=table.name,
                    alias=table.alias,
                    is_function=table.is_function,
                )
                for table in tables
            )
            return (
                Column(table_refs=tables, local_tables=stmt.local_tables),
                Function(schema=None),
                Keyword(),
            )
    elif token_v == "as":
        # Don't suggest anything for aliases
        return ()
    elif (token_v.endswith("join") and token.is_keyword) or (
        token_v in ("copy", "from", "update", "into", "describe", "truncate")
    ):
        schema = stmt.get_identifier_schema()
        tables = extract_tables(stmt.text_before_cursor)
        is_join = token_v.endswith("join") and token.is_keyword

        # Suggest tables from either the currently-selected schema or the
        # public schema if no schema has been specified
        suggest = []

        if not schema:
            # Suggest schemas
            suggest.insert(0, Schema())

        if token_v == "from" or is_join:
            suggest.append(FromClauseItem(schema=schema, table_refs=tables, local_tables=stmt.local_tables))
        elif token_v == "truncate":
            suggest.append(Table(schema))
        else:
            suggest.extend((Table(schema), View(schema)))

        if is_join and _allow_join(stmt.parsed):
            tables = stmt.get_tables("before")
            suggest.append(Join(table_refs=tables, schema=schema))

        return tuple(suggest)

    elif token_v in ("table", "view", "function"):
        # E.g. 'DROP FUNCTION <funcname>', 'ALTER TABLE <tablname>'
        rel_type = {"table": Table, "view": View, "function": Function}[token_v]
        schema = stmt.get_identifier_schema()
        if schema:
            return (rel_type(schema=schema),)
        else:
            return (Schema(), rel_type(schema=schema))

    elif token_v == "column":
        # E.g. 'ALTER TABLE foo ALTER COLUMN bar
        return (Column(table_refs=stmt.get_tables()),)

    elif token_v == "on":
        tables = stmt.get_tables("before")
        parent = (stmt.identifier and stmt.identifier.get_parent_name()) or None
        if parent:
            # "ON parent.<suggestion>"
            # parent can be either a schema name or table alias
            filteredtables = tuple(t for t in tables if identifies(parent, t))
            sugs = [
                Column(table_refs=filteredtables, local_tables=stmt.local_tables),
                Table(schema=parent),
                View(schema=parent),
                Function(schema=parent),
            ]
            if filteredtables and _allow_join_condition(stmt.parsed):
                sugs.append(JoinCondition(table_refs=tables, parent=filteredtables[-1]))
            return tuple(sugs)
        else:
            # ON <suggestion>
            # Use table alias if there is one, otherwise the table name
            aliases = tuple(t.ref for t in tables)
            if _allow_join_condition(stmt.parsed):
                return (
                    Alias(aliases=aliases),
                    JoinCondition(table_refs=tables, parent=None),
                )
            else:
                return (Alias(aliases=aliases),)

    elif token_v in ("c", "use", "database", "template"):
        # "\c <db", "use <db>", "DROP DATABASE <db>",
        # "CREATE DATABASE <newdb> WITH TEMPLATE <db>"
        return (Database(),)
    elif token_v == "schema":
        # DROP SCHEMA schema_name
        return (Schema(),)
    elif token_v.endswith(",") or token_v in ("=", "and", "or"):
        prev_keyword = stmt.reduce_to_prev_keyword()
        if prev_keyword:
            return suggest_based_on_last_token(prev_keyword, stmt)
        else:
            return ()
    elif token_v == "format":
        return (Format(),)
    elif token_v in ("type", "::"):
        #   ALTER TABLE foo SET DATA TYPE bar
        #   SELECT foo::bar
        # Note that tables are a form of composite type in postgresql, so
        # they're suggested here as well
        schema = stmt.get_identifier_schema()
        suggestions = [Datatype(schema=schema), Table(schema=schema)]
        if not schema:
            suggestions.append(Schema())
        return tuple(suggestions)
    elif token_v == "alter":
        return (Keyword(),)
    elif token.is_keyword:
        # token is a keyword we haven't implemented any special handling for
        # go backwards in the query until we find one we do recognize
        prev_keyword = stmt.reduce_to_prev_keyword(n_skip=1)
        if prev_keyword:
            return suggest_based_on_last_token(prev_keyword, stmt)
        else:
            return (Keyword(),)
    else:
        return (Keyword(),)


def identifies(id, ref):
    """Returns true if string `id` matches TableReference `ref`"""
    return id == ref.alias or id == ref.name or (ref.schema and (id == ref.schema + "." + ref.name))


def _allow_join_condition(statement):
    """
    Tests if a join condition should be suggested

    We need this to avoid bad suggestions when entering e.g.
        select * from tbl1 a join tbl2 b on a.id = <cursor>
    So check that the preceding token is a ON, AND, or OR keyword, instead of
    e.g. an equals sign.

    :param statement: an sqlparse.sql.Statement
    :return: boolean
    """

    if not statement or not statement.tokens:
        return False

    last_tok = statement.token_prev(len(statement.tokens))[1]
    return last_tok.value.lower() in ("on", "and", "or")


def _allow_join(statement):
    """
    Tests if a join should be suggested

    We need this to avoid bad suggestions when entering e.g.
        select * from tbl1 a join tbl2 b <cursor>
    So check that the preceding token is a JOIN keyword

    :param statement: an sqlparse.sql.Statement
    :return: boolean
    """

    if not statement or not statement.tokens:
        return False

    last_tok = statement.token_prev(len(statement.tokens))[1]
    return last_tok.value.lower().endswith("join") and last_tok.value.lower() not in (
        "cross join",
        "natural join",
    )


Match = namedtuple("Match", ["completion", "priority"])
_SchemaObject = namedtuple("SchemaObject", ["name", "schema", "function"])


def SchemaObject(name, schema=None, function=False):
    return _SchemaObject(name, schema, function)


_Candidate = namedtuple("Candidate", ["completion", "prio", "meta", "synonyms", "prio2"])


def Candidate(completion, prio=None, meta=None, synonyms=None, prio2=None):
    return _Candidate(completion, prio, meta, synonyms or [completion], prio2)


def normalize_ref(ref):
    return ref if ref[0] == '"' else '"' + ref.lower() + '"'


def generate_alias(tbl):
    """Generate a table alias, consisting of all upper-case letters in
    the table name, or, if there are no upper-case letters, the first letter +
    all letters preceded by _
    param tbl - unescaped name of the table to alias
    """
    return "".join(
        [c for c in tbl if c.isupper()] or [c for c, prev in zip(tbl, "_" + tbl) if prev == "_" and c != "_"]
    )
