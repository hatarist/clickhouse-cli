import os

from prompt_toolkit.buffer import AcceptAction, Buffer
from prompt_toolkit.filters import Condition
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding.manager import KeyBindingManager
from prompt_toolkit.token import Token

from clickhouse_cli.clickhouse.definitions import INTERNAL_COMMANDS
from clickhouse_cli.ui.completer import CHCompleter


KeyBinder = KeyBindingManager(enable_abort_and_exit_bindings=True, enable_search=True)


class CLIBuffer(Buffer):

    def __init__(self, client, multiline, *args, **kwargs):
        @Condition
        def is_multiline():
            if not multiline:
                return False

            text = self.document.text
            return not query_is_finished(text)

        super(CLIBuffer, self).__init__(
            *args,
            completer=CHCompleter(client),
            history=FileHistory(filename=os.path.expanduser('~/.clickhouse-cli_history')),
            enable_history_search=True,
            accept_action=AcceptAction.RETURN_DOCUMENT,
            is_multiline=is_multiline,
            **kwargs
        )


def query_is_finished(text):
    text = text.strip()
    return (
        text == '' or
        text.endswith(';') or
        text in INTERNAL_COMMANDS
    )


def get_prompt_tokens(cli):
    return [
        (Token, ' :) '),
    ]


def get_continuation_tokens(cli, width):
    return [(Token, '  ] ')]
