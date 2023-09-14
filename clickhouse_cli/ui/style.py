from click import echo_via_pager, secho
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.style import Style
from pygments.styles import get_style_by_name
from pygments.token import Comment, Error, Generic, Keyword, Name, Number, Operator, String, Token, Whitespace

RED = "#cb0f1e"
ORANGE = "#de9014"
YELLOW = "#e6cd09"
GREEN = "#21aa52"
AQUA = "#41c2b7"
BLUE = "#387be8"
PURPLE = "#860093"


class CHPygmentsStyleDefault(Style):
    background_color = "#202020"
    highlight_color = "#404040"

    styles = {
        Token: "#d0d0d0",
        Whitespace: "#666666",
        Comment: "italic #999999",
        Comment.Preproc: "noitalic bold #cd2828",
        Comment.Special: "noitalic bold #e50808 bg:#520000",
        Keyword: "bold #6ab825",
        Keyword.Pseudo: "nobold",
        Operator.Word: "bold #6ab825",
        String: "#ed9d13",
        String.Other: "#ffa500",
        Number: "#3677a9",
        Name.Builtin: "#24909d",
        Name.Variable: "#40ffff",
        Name.Constant: "#40ffff",
        Name.Class: "underline #447fcf",
        Name.Function: "#447fcf",
        Name.Namespace: "underline #447fcf",
        Name.Exception: "#bbbbbb",
        Name.Tag: "bold #6ab825",
        Name.Attribute: "#bbbbbb",
        Name.Decorator: "#ffa500",
        Generic.Heading: "bold #ffffff",
        Generic.Subheading: "underline #ffffff",
        Generic.Deleted: "#d22323",
        Generic.Inserted: "#589819",
        Generic.Error: "#d22323",
        Generic.Emph: "italic",
        Generic.Strong: "bold",
        Generic.Prompt: "#eeeeee",
        Generic.Output: "#ffffff",
        Generic.Traceback: "#d22323",
        Error: "bg:#e3d2d2 #a61717",
    }


def get_ch_pygments_style(theme=None):
    if theme is not None:
        return get_style_by_name(theme)
    return CHPygmentsStyleDefault


def get_ch_style(theme=None):
    return style_from_pygments_cls(get_ch_pygments_style(theme))


class Echo(object):
    def __init__(self, verbose=True, colors=True):
        self.verbose = verbose
        self.colors = colors

    def _echo(self, *args, **kwargs):
        if not self.colors:
            kwargs.pop("fg", None)
        if self.verbose:
            return secho(*args, **kwargs)

    def info(self, text, *args, **kwargs):
        self._echo(text, *args, **kwargs)

    def success(self, text, *args, **kwargs):
        self._echo(text, fg="green", *args, **kwargs)

    def warning(self, text, *args, **kwargs):
        self._echo(text, fg="yellow", *args, **kwargs)

    def error(self, text, *args, **kwargs):
        secho(text, fg="red", *args, **kwargs)

    def print(self, *args, **kwargs):
        if self.verbose:
            return print(*args, **kwargs)

    def pager(self, text, end=None):
        return echo_via_pager(text)
