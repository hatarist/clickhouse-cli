import re

from pygments.lexer import Lexer, RegexLexer, do_insertions, bygroups, words
from pygments.token import Punctuation, Text, Comment, Operator, Keyword, Name, String, Number, Generic, Whitespace
from pygments.lexers import get_lexer_by_name, ClassNotFound

from clickhouse_cli.clickhouse.definitions import (
    CASE_INSENSITIVE_FUNCTIONS,
    DATATYPES,
    FORMATS,
    FUNCTIONS,
    AGGREGATION_FUNCTIONS,
    KEYWORDS,
    OPERATORS,
)

line_re = re.compile('.*?\n')


class CHLexer(RegexLexer):
    """
    Lexer for the PostgreSQL dialect of SQL.

    .. versionadded:: 1.5
    """

    name = 'Clickhouse'
    aliases = ['clickhouse']
    filenames = ['*.sql']
    mimetypes = ['text/x-clickhouse-sql']

    tokens = {
        'root': [
            # (r'\s+', Text),
            # (r'--.*?\n', Comment.Single),
            # (r'/\*', Comment.Multiline, 'multiline-comments'),

            # (r'[+*/<>=~!@#%^&|`?-]+', Operator),
            # (r'\$\d+', Name.Variable),
            # (r'([0-9]*\.[0-9]*|[0-9]+)(e[+-]?[0-9]+)?', Number.Float),
            # (r'[0-9]+', Number.Integer),

            # (words(OPERATORS, prefix=r'(?i)', suffix=r'\b'), Keyword),
            # (words(DATATYPES, suffix=r'\b'), Name.Builtin),

            (r'\s+', Text),
            (r'(--\s*).*?\n', Comment),
            (r'/\*', Comment.Multiline, 'multiline-comments'),
            (r'[0-9]+', Number),
            (r'[0-9]*\.[0-9]+(e[+-][0-9]+)', Number),
            (r"'(\\\\|\\'|''|[^'])*'", String),
            (r'"(\\\\|\\"|""|[^"])*"', String),
            (r"`(\\\\|\\`|``|[^`])*`", String),
            (r'[+*/<>=~!@#%^&|`?-]', Operator),

            (words(OPERATORS, prefix=r'(?i)', suffix=r'\b'), Keyword),
            (words(DATATYPES, suffix=r'\b'), Keyword.Type),

            (words(FORMATS), Name.Label),
            (words(KEYWORDS, prefix=r'(?i)', suffix=r'\b'), Keyword),
            (words(AGGREGATION_FUNCTIONS, suffix=r'(\s*)(\()'), bygroups(Name.Function, Text, Punctuation)),
            (words(CASE_INSENSITIVE_FUNCTIONS, prefix=r'(?i)', suffix=r'\b'), Name.Function),
            (words(FUNCTIONS, suffix=r'(\s*)(\()'), bygroups(Name.Function, Text, Punctuation)),
            (r'^\\\w+', Text),

            (r'(?i)[a-z_]\w*', Text),
            (r'(?i)[;:()\[\],.]', Punctuation),

            (r"'", String.Single, 'string'),
            (r'[a-z_]\w*', Name),

            (r'[;:()\[\]{},.]', Punctuation),
        ],
        'multiline-comments': [
            (r'/\*', Comment.Multiline, 'multiline-comments'),
            (r'\*/', Comment.Multiline, '#pop'),
            (r'[^/*]+', Comment.Multiline),
            (r'[/*]', Comment.Multiline)
        ],
        'string': [
            (r"[^']+", String.Single),
            (r"''", String.Single),
            (r"'", String.Single, '#pop'),
        ],
        'quoted-ident': [
            (r'[^"]+', String.Name),
            (r'""', String.Name),
            (r'"', String.Name, '#pop'),
        ],
    }


class CHPrettyFormatLexer(RegexLexer):
    tokens = {
        'root': [
            (r'([^┌─┬┐│││└─┴┘├─┼┤]+)', Generic.Output),
            (r'([┌─┬┐│││└─┴┘├─┼┤]+)', Whitespace),
        ]
    }


class CHCSVFormatLexer(RegexLexer):
    tokens = {
        'root': [
            (r'(^"|","|",|,"|"$)', Whitespace),
            (r'""', Generic.Output),
            (r'[^",]', Generic.Output),
        ],
    }
