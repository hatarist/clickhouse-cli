from prompt_toolkit.completion import Completer, Completion

from clickhouse_cli.clickhouse.definitions import *


class CHCompleter(Completer):

    def __init__(self, client, *args, **kwargs):
        self.client = client
        super(CHCompleter, self).__init__(*args, **kwargs)

    def _match(self, word, keyword):
        match_end_limit = len(word)
        match_point = keyword.find(word, 0, match_end_limit)
        if match_point >= 0:
            return -match_point

    def _select(self, query, fmt='TabSeparated', flatten=True, *args, **kwargs):
        data = self.client.query(query, fmt=fmt).data
        return [
            row if flatten else row.split('\t') for row in data.rstrip('\n').split('\n')
        ]

    def get_completion(self, word, keywords, ignore_case=False, suffix=''):
        for keyword in keywords:
            if ignore_case:
                k = self._match(word.lower(), keyword.lower())
            else:
                k = self._match(word, keyword)

            if k is not None:
                yield Completion(keyword + suffix, -len(word))

    def get_single_match(self, word, match):
        return [Completion(match, -len(word))]

    def get_tables(self, database=None):
        if database is None:
            return self._select('SHOW TABLES')
        else:
            return self._select('SHOW TABLES FROM {}'.format(database))

    def get_databases(self):
        return self._select('SHOW DATABASES')

    def get_table_field_names(self, table, database=None):
        if database is None:
            result = self._select('DESCRIBE TABLE {}'.format(table), flatten=False)
        else:
            result = self._select('DESCRIBE TABLE {}.{}'.format(database, table), flatten=False)

        return [name for name, datatype in result]

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        word = document.get_word_before_cursor(WORD=True)

        split_text = text.split()
        count = len(split_text)

        if (count < 2 and word == '') or (count <= 2 and word != ''):
            first_keyword = split_text[0].upper() if text != '' else word

            if first_keyword == 'USE':  # USE <database>
                return self.get_completion(word, self.get_databases())
            elif first_keyword == 'INSERT':  # INSERT <INTO>
                return self.get_single_match(word.upper(), 'INTO ')
            elif first_keyword == 'SHOW':  # SHOW <DATABASES|TABLES|...>
                return self.get_completion(word.upper(), SHOW_SUBCOMMANDS)
            elif first_keyword == 'DESCRIBE':  # DESCRIBE <table>
                return self.get_completion(word, self.get_tables())
            elif first_keyword == 'CREATE':  # CREATE <TABLE|VIEW>
                return self.get_completion(word.upper(), CREATE_SUBCOMMANDS)
            elif first_keyword == 'DROP':  # DROP <DATABASE|TABLE>
                return self.get_completion(word.upper(), DROP_SUBCOMMANDS)
            elif first_keyword == 'SELECT':
                return self.get_completion(word, AGGREGATION_FUNCTIONS + FUNCTIONS)
            else:
                return self.get_completion(word.upper(), READ_QUERIES + WRITE_QUERIES, suffix=' ')
        else:
            first_keyword = split_text[0].upper()
            last_keyword = split_text[-1].upper() if word == '' else split_text[-2].upper()
            if last_keyword in ('FROM', 'INTO'):
                # SELECT * FROM <table>
                # INSERT INTO <table>
                return self.get_completion(word, self.get_tables(), suffix=' ')
            elif first_keyword in ('SHOW', 'DROP'):
                if last_keyword == 'TABLE':  # SHOW CREATE TABLE <table>, DROP TABLE <table>
                    return self.get_completion(word, self.get_tables())
                elif last_keyword == 'DATABASE':  # DROP DATABASE <database>
                    return self.get_completion(word, self.get_databases())
            elif last_keyword == 'FORMAT':
                # SELECT 1 FORMAT <format>;
                return self.get_completion(word, FORMATS)

        return self.get_completion(word, KEYWORDS + AGGREGATION_FUNCTIONS + FUNCTIONS, ignore_case=True)
