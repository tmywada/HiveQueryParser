# =================================================================================
#                      Metadata Generator from Hive Query (MGHQ)
# =================================================================================
#
import sqlparse
from io import StringIO
import re
import argparse
from pathlib import Path
from sqlparse.tokens import Keyword
from sqlparse.tokens import DML
from sqlparse.tokens import CTE
from sqlparse.tokens import DDL
from sqlparse.sql import Function
from sqlparse.sql import Where
from sqlparse.sql import IdentifierList
from sqlparse.sql import Identifier
from sqlparse.sql import Comparison
from sqlparse.sql import Parenthesis
from sqlparse.sql import Case
from sqlparse.sql import Having

# --- version
version = '0.0.1'

class Utilities:
    """
    This class containes utility functions.
    """
    def read_sql_file(self, file_path:str):
        """
        Read Hive SQL file. Add additional conditions if they are needed.

        :param str file_path: file path 
        """
        # --- initialization
        lines = []

        # --- check file
        if Path(file_path).is_file() == False:
            print(f'error: cannot access file ({file_path})')
            return None

        # --- load sql file
        with open(file_path, 'r') as f:
            for line in f:

                # --- comment line in SQL (i.e., starting with "--")
                if line.rstrip().startswith('--'):
                    continue

                # # --- comment line in Python
                # elif line.rstrip().startswith('#'):
                #     continue

                # --- brank line
                elif len(line.strip()) == 0:
                    continue

                # --- additioanl comment at lines (SELECT -- this is comment)
                elif '--' in line:
                    line = line.split('--')[0].strip()

                # ==== add condition here ====== 
                # elif ****

                # --- add line
                lines.append(line.strip())
        return '\n'.join(lines)    

    def parse_line(self, line:str):
        """
        Parse line in _pprint_tree object (e.g., |- 7 Whitespace)
        
        :param str line: a line from the _pprint_tree object
        """
        # --- split line with integer(s)
        segments = re.split('[0-9]+', line)

        # --- count number of string in the first portion
        prefix_length = len(segments[0])

        # --- remove first portion from the list
        line = line[prefix_length:]

        # --- split line by whitespace and get index
        prefix_index = int(line.split()[0].strip())

        # --- get key
        key = line.split()[1].strip()
        return prefix_length, prefix_index, key

    def elaborate_statement(self, statement:sqlparse.sql.Statement):
        """
        """
        # --- initialization
        res = []
        identifiers = []
        tokens = list(statement.tokens)

        # --- first token
        token = tokens.pop(0)
        res.append(token)

        # --- check whether group token
        if token.is_group:
            
            # --- insert flatten tokens at position 0    
            tokens = list(token.tokens) + tokens

        # --- progress
        token = tokens.pop(0)
        res.append(token)    

        # --- loop until the last token
        while len(tokens) > 0:

            # --- token is "group" (e.g., Identifier)
            if token.is_group:
                
                # --- insert flatten tokens at position 0    
                tokens = list(token.tokens) + tokens

            # --- progress
            token = tokens.pop(0)
            res.append(token)
            
            # --- cover the token after porcessing above    
            if token.is_group:
                
                # --- insert flatten tokens at position 0    
                tokens = list(token.tokens) + tokens 
                
                # --- progress
                token = tokens.pop(0)
                res.append(token)
        return res

    def parse_statement(self, statement:sqlparse.sql.Statement):
        """
        """
        # --- initialization
        output = StringIO()

        # --- store parsed output
        statement._pprint_tree(f = output)

        return output.getvalue().strip().split('\n')

    def merge_line_and_metadata(self, statement:sqlparse.sql.Statement, lines:list):
        """
        Merge objects from Token (line) and _pprint_tree (metadata)

        :param sqlparse.sql.Statement statement: object from sqlparse.parse function
        :param list lines: object from sqlparse._pprint_tree function
        """
        # --- initialization
        res = []

        # ---
        tokens = self.elaborate_statement(statement)

        # ---
        for line, token in zip(lines, tokens):

            prefix_length, prefix_index, key = self.parse_line(line)

            res.append(
                {
                    'token': key,
                    'prefix_length': prefix_length,
                    'prefix_index': prefix_index,
                    'value': token.value
                }
            )
        return res

    def preprocess_sql_query(self, query:str):
        """
        """
        # --- format query
        _query = sqlparse.format(query, reindent=True, keyword_case='upper')

        # --- parse query
        stmt, = sqlparse.parse(query)

        # --- generate lines from _pprint_tree
        lines = self.parse_statement(stmt)

        # --- get metadata and merge with lines
        res = self.merge_line_and_metadata(stmt, lines)

        return (_query, stmt, res)          

    def preprocess_sql_queries(self, queries:str):
        """
        """
        # --- initialization
        res = []

        # --- process queries
        for query in sqlparse.split(queries):

            # --- process query
            _query, stmt, _res = self.preprocess_sql_query(query)

            # --- store result
            res.append(
                {
                    'query': _query,
                    'statement': stmt,
                    'metadata_lines': _res
                }
            )
        return res

    def reset_switchs(self):
        """
        Reset switchs
        """
        self.switchs = [False] * self.num_tokens 

    def cleanup_string(self, string:str):
        """
        Clean up string. Remove newline and multiple whitespace.

        :param str string: string needs to be cleaned
        """
        # --- define pattern
        pattern = r'\s+'

        # --- replace newline with white space
        tmp = string.replace('\n', ' ')

        # --- swap defined pattern with white space (default is multiple whiste space)
        res = re.sub(pattern, ' ', tmp.strip())

        return res

class Processes(Utilities):
    """
    This class containes processes for each token that needs to process differently.
    """
    def scan_known_tokens(self, token:sqlparse.sql.Token):
        """
        select, from, grouby, ordergby, with, join, where, on
        """
        if token.ttype is DML and token.value.lower() == 'select':
            idx = self.tokens_considered.index('select')
            self.reset_switchs()
            self.switchs[idx] = True
            return

        if token.ttype is Keyword and token.value.lower() == "from":
            idx = self.tokens_considered.index('from')
            self.reset_switchs()
            self.switchs[idx] = True            
            return

        if token.ttype is Keyword and token.value.lower() == "group by":
            idx = self.tokens_considered.index('group by')
            self.reset_switchs()
            self.switchs[idx] = True   
            return

        if token.ttype is Keyword and token.value.lower() == "order by":
            idx = self.tokens_considered.index('order by')
            self.reset_switchs()
            self.switchs[idx] = True         
            return

        if token.ttype is CTE and token.value.lower() == "with":
            idx = self.tokens_considered.index('with')
            self.reset_switchs()
            self.switchs[idx] = True
            return

        if token.ttype is Keyword and token.value.lower() in ['inner join', 'join']:
            idx = self.tokens_considered.index('join')
            self.reset_switchs()
            self.switchs[idx] = True            
            return

        if token.ttype is Keyword and token.value.lower() in ['outer join', 'outer left join']:
            idx = self.tokens_considered.index('outer join')
            self.reset_switchs()
            self.switchs[idx] = True
            return     

        if token.ttype is Keyword and token.value.lower() == 'on':
            idx = self.tokens_considered.index('on')
            self.reset_switchs()
            self.switchs[idx] = True
            return

        if isinstance(token, Where):
            idx = self.tokens_considered.index('where')
            self.reset_switchs()
            self.switchs[idx] = True
            return

        if token.ttype is DDL and token.value.lower() == 'create':
            idx = self.tokens_considered.index('create')
            self.reset_switchs()
            self.switchs[idx] = True
            return

        if token.ttype is Keyword and token.value.lower() == 'having':
            idx = self.tokens_considered.index('having')
            self.reset_switchs()
            self.switchs[idx] = True
            return           

    def retrieve_column_metadata_in_select(self, token:sqlparse.sql.Token):
        """
        covering various cases
        """
        # --- retrieve base values
        column_name = token.get_name()
        column_name_before_rename = token.get_real_name()
        parent_name = token.get_parent_name()
        alias = token.get_alias()

        # --- Function (sum, count, nvl, coalesce) 
        if isinstance(token.token_first(), Function):

            #TODO reuqred additional treatment to extract info
            _res = {
                'column_name': column_name,
                'is_function': True
            }            

        # --- column name without table alias (e.g., col1)
        elif parent_name == alias == None:
            if column_name == column_name_before_rename:
                _res = {
                    'column_name': column_name
                }

        # --- column name with alias (e.g., a.col1)
        elif alias == None:
            _res = {
                'column_name': column_name,
                'table_alias': parent_name
            }

        # --- Operation, Float, Integer, etc. -> "is_others"
        elif parent_name == None:
            if alias == column_name == column_name_before_rename:
                _res = {
                    'column_name': column_name,
                    'is_others': True
                }

        # --- renamed (e.g., a.col1 as col1_renamed)
        elif column_name == alias:
            _res = {
                'column_name': column_name,
                'column_name_before_rename': column_name_before_rename,
                'table_alias': parent_name
            }

        return _res

    def retrieve_column_metadata_in_orderby(self, token:sqlparse.sql.Token):
        """
        """
        with_ordering = False
        try:
            ordering = token.get_ordering()
            if with_ordering != None:
                with_ordering = True
        except:
            pass

        if with_ordering:
            token = token.tokens[0]

        _res = {
            'column_name': token.get_name(),
            'table_alias': token.get_parent_name()
        }

        return _res    

    def retrieve_column_metadata(self, token:sqlparse.sql.Token):
        """
        this is only column name
        """
        # --- retrieve base values
        column_name = token.get_name()
        column_name_before_rename = token.get_real_name()
        parent_name = token.get_parent_name()
        alias = token.get_alias()

        # --- column name without table alias (e.g., col1)
        if parent_name == alias == None:
            if column_name == column_name_before_rename:
                _res = {
                    'column_name': column_name
                }

        # --- column name with alias (e.g., a.col1)
        elif alias == None:
            _res = {
                'column_name': column_name,
                'parent_name': parent_name
            }

        # --- renamed (e.g., a.col1 as col1_renamed)
        elif column_name == alias:
            _res = {
                'column_name': column_name,
                'column_name_before_rename': column_name_before_rename,
                'parent_name': parent_name
            }

        return _res

    def retrieve_table_metadata(self, token:sqlparse.sql.Token):
        """
        """
        res = {
            'schema_name': token.get_parent_name(),
            'table_name':  token.get_real_name(),
            'table_alias': token.get_alias()
        }
        return res

    def retrieve_temporary_table_metadata(self, token:sqlparse.sql.Token):
        """
        """
        # --- get alias
        table_alias = token.get_name()

        # --- process inside of Parenthesis (last token)
        _query = self.cleanup_string(token.tokens[-1].value[1:-1])
        _query += ';'

        _statement, = sqlparse.parse(_query)

        parser_sub = GenerateMetadataHiveQueries()
        res_tmp_table = parser_sub.analyse_query(_statement)

        res = {
                'token': 'with',
                'type':  'temporary table',
                'table_alias': table_alias,
                'value': res_tmp_table
        }
        return res      

    def retrieve_metadata_case(self, token:sqlparse.sql.Token):
        """
        """
        # --- parse case statement 
        #     [
        #       ( [key, condition], [key, value] ),  <--- when, then
        #       ( None, [key, value] )               <--- else
        #     ]

        # --- initialization
        _res = token.get_cases(skip_ws = True)
        is_nested = False
        res = []

        # --- split dictionaries (when&then and else)
        tmp_when_then = _res[0]
        tmp_else      = _res[1]    

        # --- when
        for _token in tmp_when_then[0]:
            if _token.ttype is Keyword:
                continue
            res.append(
                {
                    'when': _token.value
                }
            )

        # --- then
        res.append(
            {
                'then': tmp_when_then[1][1].value
            }
        )
        # --- else
        if isinstance(tmp_else[1][1], Case):
            res.append(
                {
                    'else': 'nested_case'
                }
            )
            is_nested = True
            token_nested = tmp_else[1][1]
        else:
            res.append(
                {
                    'else': tmp_else[1][1].value
                }
            )
            token_nested = None

        return (is_nested, res, token_nested)      

    def process_identifier_select(self, token:sqlparse.sql.Token):
        """
        """
        # --- case
        if isinstance(token.tokens[0], Case):

            _res_case = []

            _is_nested, tmp, token_nested = self.retrieve_metadata_case(token.tokens[0])

            if _is_nested:
                is_nested = True
            else:
                is_nested = False

            # --- column name
            tmp.append(
                {
                    'column_name': token.get_name()
                }
            )
            _res_case.extend(tmp)

            # --- nested
            while _is_nested:
                _is_nested, tmp, token_nested = self.retrieve_metadata_case(token_nested)
                _res_case.append(tmp)

            res = {
                'token': 'select',
                'type':  'case'
            }

            if is_nested:
                res['is_nested'] = True
            else:
                res['is_nested'] = False

            res['metadta'] = _res_case

        # --- others
        else:
            res = {
                'token': 'select',
                'type':  'column',
                'value': self.cleanup_string(token.value),
                'metadata': self.retrieve_column_metadata_in_select(token)
            }
        return res   

    def process_select(self, token:sqlparse.sql.Token):
        """
        """

        res = []

        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():

                res.append( self.process_identifier_select(identifier) )

        elif isinstance(token, Identifier):
 
            res.append( self.process_identifier_select(token) )

        return res

    def process_from(self, token:sqlparse.sql.Token):
        """
        """
        res = []

        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():

                _res = {
                    'token': 'FROM',
                    'type':  'table',
                    'value': self.cleanup_string(identifier.value),
                    'metadata': self.retrieve_table_metadata(identifier)
                }           

                res.append( _res )

        elif isinstance(token, Identifier):

            # --- alias
            with_alias = False
            try:
                alias = token.get_alias()
                if alias != None:
                    with_alias = True
            except:
                pass

            if token.value[0] == '(':

                _query = self.cleanup_string(token.tokens[0].value[1:-1])
                _query += ';'

                _statement, = sqlparse.parse(_query)

                parser_sub = GenerateMetadataHiveQueries()
                res_sub = parser_sub.analyse_query(_statement)

                _res = {
                    'token': 'FROM',
                    'type':  'subquery',
                    'value': res_sub
                }

                if with_alias:
                    _res['alias'] = alias

            # --- simple form (e.g., schema.table as a)
            else:
                _res = {
                    'token': 'FROM',
                    'type':  'table',
                    'value': self.cleanup_string(token.value),
                    'metadata': self.retrieve_table_metadata(token)
                }                          

            res.append( _res )

        elif isinstance(token, Parenthesis):

            # --- alias
            with_alias = False
            try:
                alias = token.get_alias()
                if alias != None:
                    with_alias = True
            except:
                pass

            _query = self.cleanup_string(
                ' '.join([v.value for v in token.tokens[1:-1]])
            )
            _query += ';'

            _statement, = sqlparse.parse(_query)

            parser_sub = GenerateMetadataHiveQueries()
            res_sub = parser_sub.analyse_query(_statement)

            _res = {
                'token': 'FROM',
                'type':  'subquery',
                'value': res_sub
            }                          

            if with_alias:
                _res['alias'] = alias

            res.append( _res )

        return res        

    def process_grouby(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                res.append(
                    {
                        'token': 'GROUP BY',
                        'type':  'column',
                        'value': self.cleanup_string(identifier.value),
                        'metadata': self.retrieve_column_metadata_in_select(identifier)
                    }
                )

        elif isinstance(token, Identifier):
            res.append(
                {
                    'token': 'GROUP BY',
                    'type':  'column',
                    'value': self.cleanup_string(token.value),
                    'metadata': self.retrieve_column_metadata_in_select(token)
                }
            )
        return res

    def process_orderby(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                res.append(
                    {
                        'token': 'ORDER BY',
                        'type':  'multiple',
                        'value': self.cleanup_string(identifier.value),
                        'metadata': self.retrieve_column_metadata_in_orderby(identifier)
                    }
                )

        elif isinstance(token, Identifier):
            res.append(
                {
                    'token': 'ORDER BY',
                    'type':  'single',
                    'value': self.cleanup_string(token.value),
                    'metadata': self.retrieve_column_metadata_in_orderby(token)
                }
            )
        return res

    def process_where(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, Comparison):
            res.append(
                {
                    'token': 'WHERE',
                    'type':  'comparison',
                    'value': self.cleanup_string(token.value)
                }
            )
        elif isinstance(token, Parenthesis):
            res.append(
                {
                    'token': 'WHERE',
                    'type':  'parenthesis',
                    'value': self.cleanup_string(token.value)
                }
            )
        return res            

    def process_inner_join(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, Identifier):

            _res = {
                'token': 'join',
                'type':  'table',
                'value': self.cleanup_string(token.value),
                'metadata': self.retrieve_table_metadata(token)
            }                

            res.append( _res )

        return res

    def process_outer_join(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, Identifier):

            _res = {
                'token': 'outer join',
                'type':  'table',
                'value': self.cleanup_string(token.value),
                'metadata': self.retrieve_table_metadata(token)
            }         

            res.append( _res )

        return res

    def process_on(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, Comparison):
            res.append(
                {
                    'token': 'ON',
                    'type':  'comparison',
                    'value': self.cleanup_string(token.value)
                }
            )
        return res  

    def process_with(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, Identifier):

            res.append(self.retrieve_temporary_table_metadata(token))

        elif isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():

                res.append( self.retrieve_temporary_table_metadata(token) )

        return res

    def process_create(self, token:sqlparse.sql.Token):
        """
        """
        res = []
        if isinstance(token, Identifier):

            res.append(
                {
                    'token': 'create',
                    'type':  'table',
                    'value': token.value,
                    'metadta': self.retrieve_table_metadata(token)
                }
            )

        return res

    def process_having(self, token:sqlparse.sql.Token):
        """
        Not yet "col IS NOT NULL"
        """
        res = []
        if isinstance(token, Comparison):
            res.append(
                {
                    'token': 'having',
                    'type':  'comparison',
                    'value': self.cleanup_string(token.value)
                }
            )
        return res   

class GenerateMetadataHiveQueries(Processes):
    """
    This class contains a wrapper functions.
    """
    def __init__(self):
        """
        Need to be sync
        - tokens listed in `tokens_considered`
        - processes in the `Processes` class
        - if statement in `analyse_token` 
        """
        # --- define tokens
        self.tokens_considered = [
            'create',
            'from',
            'group by',
            'having',
            'join',
            'on',
            'order by',
            'outer join',
            'select',
            'where',
            'with'
        ]

        self.num_tokens = len(self.tokens_considered)

        # --- initialization
        self.switchs = [False] * len( self.tokens_considered )

    def analyse_token(self, token:sqlparse.sql.Token):
        """
        """
        # --- initialization
        _res = []

        # --- select
        if self.switchs[self.tokens_considered.index('select')]:
            _res.extend(self.process_select(token))
            return _res

        # --- from
        if self.switchs[self.tokens_considered.index('from')]:
            _res.extend(self.process_from(token)) 
            return _res          

        # --- group by
        if self.switchs[self.tokens_considered.index('group by')]:
            _res.extend(self.process_grouby(token))
            return _res 

        # --- order by
        if self.switchs[self.tokens_considered.index('order by')]:
            _res.extend(self.process_orderby(token))
            return _res

        # --- where
        if self.switchs[self.tokens_considered.index('where')]:
            _res.extend(self.process_where(token))
            return _res   

        # --- join (inner join)
        if self.switchs[self.tokens_considered.index('join')]:
            _res.extend(self.process_inner_join(token))
            return _res 

        # --- outer join
        if self.switchs[self.tokens_considered.index('outer join')]:
            _res.extend(self.process_outer_join(token))
            return _res           

        # --- on
        if self.switchs[self.tokens_considered.index('on')]:
            _res.extend(self.process_on(token))
            return _res

        # --- temporary table
        if self.switchs[self.tokens_considered.index('with')]:
            _res.extend(self.process_with(token))
            return _res        

        # --- create
        if self.switchs[self.tokens_considered.index('create')]:
            _res.extend(self.process_create(token))
            return _res  

        # --- having
        if self.switchs[self.tokens_considered.index('having')]:
            _res.extend(self.process_having(token))
            return _res          

        return _res

    def analyse_query(self, statement:sqlparse.sql.Statement):
        """
        """
        # --- initialization
        res = []
        tokens = statement.tokens
        idx = -1

        while idx < len(tokens) - 1:
            idx += 1
            token = tokens[idx]

            # --- scan token
            self.scan_known_tokens(token)

            res.extend( self.analyse_token(token) )

        return res

def generate_metadata_from_hive_query(file_path:str, idx_query:int):
    """
    Generate metadata from a hive query.

    :param str file_path: SQL file path
    :param int idx_query: index of target query to generate metadata
    """
    # --- initialization
    parser  = GenerateMetadataHiveQueries()

    # --- define parameters
    if file_path == None:
        file_path = './data/sample.sql'

    if idx_query == None:
        idx_query = 0

    # --- preprocess
    _res = parser.preprocess_sql_queries(parser.read_sql_file(file_path))

    # --- parse query (index of "idx_query")
    stmt = _res[idx_query]['statement']

    # --- prepare output
    result = {
            'query': _res[idx_query]['query'],
            'statement': stmt,
            'metadata_lines': _res[idx_query]['metadata_lines'],
            'metadata_query': parser.analyse_query(stmt)
    }
    return [result]

def generate_metadata_from_hive_queries(file_path:str):
    """
    Generae metadta from multiple hive queries.

    :param str file_path: SQL file path
    """
    # --- initialization
    parser  = GenerateMetadataHiveQueries()
    results = []

    # --- substitute parameter if they are None
    if file_path == None:
        file_path = './data/sample.sql'

    # --- preprocess
    _res = parser.preprocess_sql_queries(parser.read_sql_file(file_path))

    # ---
    for idx_query in range(len(_res)):

        stmt = _res[idx_query]['statement']

        results.append(
            {
                'query': _res[idx_query]['query'],
                'statement': stmt,
                'metadata_lines': _res[idx_query]['metadata_lines'],
                'metadata_query': parser.analyse_query(stmt)
            }
        )

    return results

if __name__ == '__main__':

    # --- initialization
    arg_parser = argparse.ArgumentParser()

    # --- load parameters
    arg_parser.add_argument('--file_path', type=str)
    arg_parser.add_argument('--idx_query', type=int, default = 0)

    # --- parser arguments
    options = arg_parser.parse_args()

    # --- single query
    res = generate_metadata_from_hive_query(
        file_path = options.file_path,
        idx_query = options.idx_query
    )

    # # --- multple queries   
    # res = generate_metadata_from_hive_queries(
    #     file_path = file_path
    # )

    print(res[0]['query'])
    print('---')
    print(res[0]['metadata_query'])

