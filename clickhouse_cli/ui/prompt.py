import os

from prompt_toolkit.buffer import AcceptAction, Buffer
from prompt_toolkit.enums import DEFAULT_BUFFER, SEARCH_BUFFER
from prompt_toolkit.filters import Condition, HasFocus
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding.manager import KeyBindingManager
from prompt_toolkit.keys import Keys
from prompt_toolkit.token import Token

from clickhouse_cli.clickhouse.definitions import INTERNAL_COMMANDS
from clickhouse_cli.ui.completer import CHCompleter


KeyBinder = KeyBindingManager.for_prompt()


class CLIBuffer(Buffer):

    def __init__(self, client, multiline, metadata, *args, **kwargs):
        @Condition
        def is_multiline():
            if not multiline:
                return False

            text = self.document.text
            return not query_is_finished(text, multiline)

        super(CLIBuffer, self).__init__(
            *args,
            completer=CHCompleter(client, metadata),
            history=FileHistory(
                filename=os.path.expanduser('~/.clickhouse-cli_history')
            ),
            enable_history_search=True,
            accept_action=AcceptAction.RETURN_DOCUMENT,
            is_multiline=is_multiline,
            **kwargs
        )


def query_is_finished(text, multiline=False):
    text = text.strip()
    return (
        (not multiline and text == '') or
        text.endswith(';') or
        text in INTERNAL_COMMANDS
    )


def get_prompt_tokens(cli):
    return [
        (Token.Prompt, ' :) '),
    ]


def get_continuation_tokens(cli, width):
    return [(Token.Prompt, '  ] ')]


@KeyBinder.registry.add_binding(Keys.ControlC, filter=HasFocus(DEFAULT_BUFFER))
def reset_buffer(event):
    buffer = event.current_buffer
    if buffer.complete_state:
        buffer.cancel_completion()
    else:
        buffer.reset()


@KeyBinder.registry.add_binding(Keys.ControlC, filter=HasFocus(SEARCH_BUFFER))
def reset_search_buffer(event):
    if event.current_buffer.document.text:
        event.current_buffer.reset()
    else:
        event.cli.push_focus(DEFAULT_BUFFER)


