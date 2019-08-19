import os

from prompt_toolkit.application import get_app
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.document import Document
from prompt_toolkit.enums import DEFAULT_BUFFER, SEARCH_BUFFER
from prompt_toolkit.filters import Condition, HasFocus
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from pygments.token import Token
# from prompt_toolkit.formatted_text import PygmentsTokens

from clickhouse_cli.clickhouse.definitions import INTERNAL_COMMANDS
from clickhouse_cli.ui.completer import CHCompleter


kb = KeyBindings()


def is_multiline(multiline):
    @Condition
    def cond():
        doc = get_app().layout.get_buffer_by_name(DEFAULT_BUFFER).document
        if not multiline:
            return False
        else:
            return not query_is_finished(doc.text)

    return cond


class CLIBuffer(Buffer):

    def __init__(self, client, multiline, metadata, *args, **kwargs):
        super(CLIBuffer, self).__init__(
            *args,
            completer=CHCompleter(client, metadata),
            enable_history_search=True,
            # doesn't seem to have any effect on prompt_toolkit 2.x's PromptSession
            # multiline=is_multiline(multiline),
            **kwargs
        )


def query_is_finished(text, multiline=False):
    text = text.strip()
    return (
        (not multiline and text == '') or
        text.endswith(';') or
        text in INTERNAL_COMMANDS
    )


def get_prompt_tokens(*args):
    return [
        (Token.Prompt, ' :) '),
    ]


def get_continuation_tokens(*args):
    return [(Token.Prompt, '  ] ')]


@kb.add(Keys.ControlC, filter=HasFocus(DEFAULT_BUFFER))
def reset_buffer(event):
    buffer = event.app.current_buffer
    if buffer.complete_state:
        buffer.cancel_completion()
    else:
        buffer.reset()


@kb.add(Keys.ControlC, filter=HasFocus(SEARCH_BUFFER))
def reset_search_buffer(event):
    buffer = event.app.current_buffer
    if buffer.document.text:
        buffer.reset()
    else:
        event.cli.push_focus(DEFAULT_BUFFER)


@kb.add('tab')
def autocomplete(event):
    """Force autocompletion at cursor."""
    buffer = event.app.current_buffer
    if buffer.complete_state:
        buffer.complete_next()
    else:
        buffer.start_completion(select_first=True)


@kb.add('c-space')
def autocomplete_ctrl_space(event):
    buffer = event.app.current_buffer
    if buffer.complete_state:
        buffer.complete_next()
    else:
        buffer.start_completion(select_first=False)
