from prompt_toolkit.completion import Completer, Completion

from clickhouse_cli.clickhouse.client import DBException
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
        try:
            if database is None:
                result = self._select('DESCRIBE TABLE {}'.format(table), flatten=False)
            else:
                result = self._select('DESCRIBE TABLE {}.{}'.format(database, table), flatten=False)
        except DBException:
            return []
        return [field[0] for field in result]

    def get_field_completions(self, document):
        text = document.text_after_cursor
        word = document.get_word_before_cursor(WORD=True)
        split_text = text.split()
        split_text_uppercase = text.upper().split()
        try:
            from_pos = split_text_uppercase.index('FROM')
        except ValueError:
            # No FROM keyword found
            return

        if len(split_text) >= 2 and len(split_text) >= from_pos + 2:
            table_name = split_text[from_pos + 1]
            if table_name[0] == '(':
                # TODO: add completion support with the columns from the subquery
                # SELECT <field1> FROM (SELECT <field1>, <field2> FROM table);
                pass
            else:
                # SELECT <field1>, <field2>, <fieldN> FROM table;
                table_name = table_name.rstrip(';)')

                if word.startswith(AGGREGATION_FUNCTIONS + FUNCTIONS) and not word.lower().startswith('count') and '(' in word:
                    # SELECT any(<field>) FROM table;
                    return self.get_completion(word[word.index('(') + 1:], self.get_table_field_names(table_name))

                return self.get_completion(word, self.get_table_field_names(table_name))

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        word = document.get_word_before_cursor(WORD=True)

        split_text = text.split()
        count = len(split_text)

        if (count < 2 and word == '') or (count <= 2 and word != ''):
            first_keyword = split_text[0].upper() if text != '' else word

            if first_keyword in ('USE', '\C'):  # USE <database>
                return self.get_completion(word, self.get_databases())
            elif first_keyword == '\D+':  # \d+ <table>
                return self.get_completion(word, self.get_tables())
            elif first_keyword == 'INSERT':  # INSERT <INTO>
                return self.get_single_match(word.upper(), 'INTO ')
            elif first_keyword == 'SHOW':  # SHOW <DATABASES|TABLES|...>
                return self.get_completion(word.upper(), SHOW_SUBCOMMANDS)
            elif first_keyword in ('DESC', 'DESCRIBE', 'OPTIMIZE'):  # DESCRIBE <table>
                return self.get_completion(word, self.get_tables())
            elif first_keyword == 'CREATE':  # CREATE <TABLE|VIEW>
                return self.get_completion(word.upper(), CREATE_SUBCOMMANDS)
            elif first_keyword == 'DROP':  # DROP <DATABASE|TABLE>
                return self.get_completion(word.upper(), DROP_SUBCOMMANDS)
            elif first_keyword == 'SELECT':
                field_completion = self.get_field_completions(document)

                if field_completion:
                    return field_completion

                return self.get_completion(word, AGGREGATION_FUNCTIONS + FUNCTIONS)
            else:
                return self.get_completion(word.upper(), READ_QUERIES + WRITE_QUERIES)
        else:
            first_keyword = split_text[0].upper()
            last_keyword = split_text[-1].upper() if word == '' else split_text[-2].upper()

            if first_keyword == 'SELECT':
                field_completion = self.get_field_completions(document)

                if field_completion:
                    return field_completion

            if last_keyword in ('FROM', 'INTO'):
                # SELECT * FROM <table>
                # INSERT INTO <table>
                return self.get_completion(word, self.get_tables())
            elif first_keyword in ('SHOW', 'DROP', 'ALTER', 'RENAME'):
                if last_keyword == 'TABLE':  # SHOW CREATE TABLE <table>, DROP TABLE <table>
                    return self.get_completion(word, self.get_tables())
                elif last_keyword == 'DATABASE':  # DROP DATABASE <database>
                    return self.get_completion(word, self.get_databases())
            elif last_keyword == 'FORMAT':
                # SELECT 1 FORMAT <format>;
                return self.get_completion(word, FORMATS)

        return self.get_completion(word, KEYWORDS + AGGREGATION_FUNCTIONS + FUNCTIONS, ignore_case=True)
