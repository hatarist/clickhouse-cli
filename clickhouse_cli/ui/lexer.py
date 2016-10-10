from prompt_toolkit.token import Token
from pygments.lexer import RegexLexer, bygroups, words

from clickhouse_cli.clickhouse.definitions import (
    CASE_INSENSITIVE_FUNCTIONS,
    DATATYPES,
    FORMATS,
    FUNCTIONS,
    KEYWORDS,
    OPERATORS,
)


class CHLexer(RegexLexer):
    name = 'Clickhouse'
    aliases = ['clickhouse']
    filenames = ['*.sql']
    mimetypes = ['text/x-clickhouse-sql']

    tokens = {
        'root': [
            (r'\s+', Token.Text),
            (r'(--\s*).*?\n', Token.Comment.Single),
            (r'/\*', Token.Comment.Multiline, 'multiline-comments'),
            (r'[0-9]+', Token.Number),
            (r'[0-9]*\.[0-9]+(e[+-][0-9]+)', Token.Number),
            (r"'(\\\\|\\'|''|[^'])*'", Token.String),
            (r'"(\\\\|\\"|""|[^"])*"', Token.String),
            (r"`(\\\\|\\`|``|[^`])*`", Token.String),
            (r'[+*/<>=~!@#%^&|`?-]', Token.Operator),

            (words(OPERATORS, prefix=r'(?i)', suffix=r'\b'), Token.Keyword),
            (words(DATATYPES, suffix=r'\b'), Token.Keyword.Type),

            (words(FORMATS), Token.Keyword.Format),
            (words(KEYWORDS, prefix=r'(?i)', suffix=r'\b'), Token.Keyword),
            (words(CASE_INSENSITIVE_FUNCTIONS, prefix=r'(?i)', suffix=r'\b'), Token.Name.Function),
            (words(FUNCTIONS, suffix=r'(\s*)(\()'), bygroups(Token.Name.Function, Token.Text, Token.Punctuation)),

            (r'(?i)[a-z_]\w*', Token.Text),
            (r'(?i)[;:()\[\],.]', Token.Punctuation)
        ],
        'multiline-comments': [
            (r'/\*', Token.Comment.Multiline, 'multiline-comments'),
            (r'\*/', Token.Comment.Multiline, '#pop'),
            (r'[^/*]+', Token.Comment.Multiline),
            (r'[/*]', Token.Comment.Multiline)
        ]
    }
