from click import secho
from prompt_toolkit.styles import style_from_dict
from prompt_toolkit.token import Token


RED = "#cb0f1e"
ORANGE = "#de9014"
YELLOW = "#e6cd09"
GREEN = "#21aa52"
AQUA = "#41c2b7"
BLUE = "#387be8"
PURPLE = "#860093"


CHStyle = style_from_dict({
    # Token.Type: AQUA,
    # Token.Format: AQUA,
    # Token.Keyword: BLUE,
    # Token.String: YELLOW,
    # Token.Name: GREEN,
    # Token.Pattern: RED,
    # Token.Number: PURPLE,
    Token.Whitespace: '#888888',
    Token.Token: '#ffffff',
    Token.Text: '#ffffff',
    Token.Generic.Output: '#444444 bg:#222222',
    Token.Keyword: '#fb660a',
    Token.Number: '#0086f7',
    Token.Name: '#fb660a',
    Token.Comment: '#008800 bg:#0f140f italic',
    Token.Name.Attribute: '#ff0086 bold',
    Token.String: '#0086d2',
    Token.Name.Function: '#ff0086 bold',
    Token.Generic.Heading: '#ffffff bold',
    Token.Generic.Subheading: '#ffffff bold',
    Token.Keyword.Type: '#21ce1d bold',
    Token.Keyword.Format: '#21ce1d bold',
    Token.Comment.Preproc: '#ff0007 bold'
})


class Echo(object):

    def __init__(self, verbose=True):
        self.verbose = verbose

    def _echo(self, *args, **kwargs):
        if self.verbose:
            return secho(*args, **kwargs)

    def info(self, text, *args, **kwargs):
        self._echo(text, *args, **kwargs)

    def success(self, text, *args, **kwargs):
        self._echo(text, fg='green', *args, **kwargs)

    def warning(self, text, *args, **kwargs):
        self._echo(text, fg='yellow', *args, **kwargs)

    def error(self, text, *args, **kwargs):
        self._echo(text, fg='red', *args, **kwargs)

    def print(self, *args, **kwargs):
        if self.verbose:
            return print(*args, **kwargs)
