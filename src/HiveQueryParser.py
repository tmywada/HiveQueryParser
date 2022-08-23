# =================================================================================
#
#                             Hive SQL parser
#
#                                              Written by Tomoya Wada
#
# =================================================================================
#
import sqlparse
from io import StringIO
import re
import copy
import pandas as pd
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

def read_sql_file(file_path:str):
    """
    """
    lines = []
    # --- load sql file
    with open(file_path, 'r') as f:
        for line in f:

            # --- comment line in SQL
            if line.rstrip().startswith('--'):
                continue

            # --- comment line in Python
            elif line.rstrip().startswith('#'):
                continue

            # --- brank line
            elif len(line.strip()) == 0:
                continue

            # --- additioanl comment line in SQL (SELECT -- this is comment)
            elif '--' in line:
                line = line.split('--')[0].strip()

            # -- add condition here
            #

            # --- add line
            lines.append(line.strip())
    return '\n'.join(lines)    

class Utilities:

    def parse_line(self, line:str):
        """
        |- 7 Whitespace 
        """
        # --- split line with integer(s)
        tmp = re.split('[0-9]+', line)

        # --- count number of string in the first portion
        prefix_length = len(tmp[0])

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

    def parse_query(self, statement:sqlparse.sql.Statement, lines:list):
        """
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

    def preprocess_sql_file(self, input_string:str):
        """
        """
        # --- initialization
        res = []

        # --- process queries
        for query in sqlparse.split(input_string):

            # --- format query
            _query = sqlparse.format(query, reindent=True, keyword_case='upper')

            # --- parse query
            statement, = sqlparse.parse(query)

            # --- generate lines from _pprint_tree
            lines = self.parse_statement(statement)

            # --- get metadata
            _res = self.parse_query(statement, lines)

            # --- store result
            res.append(
                {
                    'query': _query,
                    'statement': statement,
                    'metadata': _res
                }
            )
        return res

class Processes:

    def __init__(self):
        """
        """
        # --- define tokens
        self.tokens_considered = [
            'create',
            'from',
            'group by',
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

    def process_select(self, token:sqlparse.sql.Token):
        """
        """

        res = []
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():

                _res = {
                    'token': 'select',
                    'type':  'column',
                    'value': self.cleanup_string(identifier.value),
                    'metadata': self.retrieve_column_metadata_in_select(identifier)
                }

                res.append( _res )

        elif isinstance(token, Identifier):

            _res = {
                'token': 'select',
                'type':  'column',
                'value': self.cleanup_string(identifier.value),
                'metadata': self.retrieve_column_metadata_in_select(token)
            }

            res.append( _res )

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

                parser_sub = ParseHiveQuery()
                res_sub = parser_sub.parse_query(_statement)

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

            parser_sub = ParseHiveQuery()
            res_sub = parser_sub.parse_query(_statement)

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
        for _token in token:
            if isinstance(_token, Comparison):
                res.append(
                    {
                        'token': 'WHERE',
                        'type':  'comparison',
                        'value': self.cleanup_string(_token.value)
                    }
                )
            elif isinstance(_token, Parenthesis):
                res.append(
                    {
                        'token': 'WHERE',
                        'type':  'parenthesis',
                        'value': self.cleanup_string(_token.value)
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

            # --- get alias
            table_alias = token.get_name()

            # --- process inside of Parenthesis (last token)
            _query = self.cleanup_string(token.tokens[-1].value[1:-1])
            _query += ';'

            _statement, = sqlparse.parse(_query)

            parser_sub = ParseHiveQuery()
            res_tmp_table = parser_sub.parse_query(_statement)

            res.append(
                {
                    'token': 'with',
                    'type':  'temporary table',
                    'table_alias': table_alias,
                    'value': res_tmp_table
                }
            )

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

class ParseHiveQuery(Processes):

    def reset_switchs(self):
        """
        """
        self.switchs = [False] * self.num_tokens 

    def cleanup_string(self, string:str):
        """
        remove newline and multiple whitespace
        """
        # --- define pattern
        pattern = r'\s+'

        # --- replace newline with white space
        tmp = string.replace('\n', ' ')

        # --- swap defined pattern with white space (default is multiple whiste space)
        res = re.sub(pattern, ' ', tmp.strip())

        return res

    def analyse_token(self, token:sqlparse.sql.Token):
        """
        """
        # --- initialization
        _res = []

        # --- SELECT
        if self.switchs[self.tokens_considered.index('select')]:
            _res.extend(self.process_select(token))
            return _res

        # --- FROM
        if self.switchs[self.tokens_considered.index('from')]:
            _res.extend(self.process_from(token)) 
            return _res          

        # --- GROUP BY
        if self.switchs[self.tokens_considered.index('group by')]:
            _res.extend(self.process_grouby(token))
            return _res 

        # --- ORDER BY
        if self.switchs[self.tokens_considered.index('order by')]:
            _res.extend(self.process_orderby(token))
            return _res

        # --- WHERE
        if self.switchs[self.tokens_considered.index('where')]:
            _res.extend(self.process_where(token))
            return _res   

        # --- JOIN
        if self.switchs[self.tokens_considered.index('join')]:
            _res.extend(self.process_inner_join(token))
            return _res 

        # --- OUTER JOIN
        if self.switchs[self.tokens_considered.index('outer join')]:
            _res.extend(self.process_outer_join(token))
            return _res           

        # --- ON
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

        return _res

    def parse_query(self, statement:sqlparse.sql.Statement):
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

    # def parse_query_sub(self, tokens):
    #     """
    #     """
    #     # --- initialization
    #     res_sub = []
    #     idx_sub = -1

    #     while idx_sub < len(tokens) - 1:
    #         idx_sub += 1
    #         token = tokens[idx_sub]

    #         # --- scan token
    #         self.scan_known_tokens(token)

    #         res_sub.extend( self.analyse_token(token) )

    #     return res_sub



# #-------------
# # --- io
# string_input = read_sql_file('./data/sample.sql')
# res = preprocess_sql_file(string_input)
# idx = 8

# # --- wip code

# i = -1
# data = res[idx]['metadata']

# # items = ['IdentifierList', 'Identifier', 'Function', 'Parenthesis', 'Where']
# tokens_aggregated = ['IdentifierList', 'Identifier', 'Where']
# tokens_aggregate_criteria = {'Keyword':'group by', 'CTE':'with'}
# tokens_skip = ['Whitespace', 'Newline']

# test = []

# while i < len(data):
    
#     i += 1
#     if i == len(data):
#         break
        
#     item = data[i]
    
#     length = item['prefix_length']
#     token  = item['token']
#     value  = item['value']
    
#     # --- skip 
#     while token in tokens_skip:
#         i += 1
#         if i == len(data):
#             break
#         item = data[i]

#         length = item['prefix_length']
#         token  = item['token']
#         value  = item['value']
    
#     _items = []
    
#     if token in tokens_aggregated:

#         # --- reference
#         item_ref = copy.deepcopy(item)
#         length_ref = copy.deepcopy(length)
        
#         # --- progress
#         i += 1
#         if i == len(data):
#             break
        
#         item = data[i]

#         length = item['prefix_length']
#         token  = item['token']
#         value  = item['value']  
        
#         # --- skip 
#         while token in tokens_skip:
#             i += 1
#             if i == len(data):
#                 break
#             item = data[i]

#             length = item['prefix_length']
#             token  = item['token']
#             value  = item['value']
        
#         _items.append(item)
        
#         while length > length_ref:
            
#             # --- progress
#             i += 1
#             if i == len(data):
#                 break
            
#             item = data[i]

#             length = item['prefix_length']
#             token  = item['token']
#             value  = item['value']  
            
#             # --- skip 
#             while token in tokens_skip:
#                 i += 1
#                 if i == len(data):
#                     break
#                 item = data[i]

#                 length = item['prefix_length']
#                 token  = item['token']
#                 value  = item['value']
            
#             _items.append(item)
                
#         test.append(
#             {
#                 'token': item_ref['token'],
#                 'item_ref': item_ref,
#                 'items': _items
#             }
        
#         )
        
#     elif token in tokens_aggregate_criteria and value == tokens_aggregate_criteria[token]:

#         # --- reference
#         item_ref = copy.deepcopy(item)
#         length_ref = copy.deepcopy(length)

#         # --- progress
#         i += 1
#         if i == len(data):
#             break
        
#         item = data[i]

#         length = item['prefix_length']
#         token  = item['token']
#         value  = item['value']  
        
#         # --- skip 
#         while token in tokens_skip:
#             i += 1
#             if i == len(data):
#                 break
#             item = data[i]

#             length = item['prefix_length']
#             token  = item['token']
#             value  = item['value']
        
#         _items.append(item)

#         # --- first 
#         while length == length_ref:
            
#             # --- progress
#             i += 1
#             if i == len(data):
#                 break
            
#             item = data[i]

#             length = item['prefix_length']
#             token  = item['token']
#             value  = item['value']  
            
#             # --- skip 
#             while token in tokens_skip:
#                 i += 1
#                 if i == len(data):
#                     break
#                 item = data[i]

#                 length = item['prefix_length']
#                 token  = item['token']
#                 value  = item['value']
            
#             _items.append(item)  

#         # --- group
#         while length > length_ref:
            
#             # --- progress
#             i += 1
#             if i == len(data):
#                 break
            
#             item = data[i]

#             length = item['prefix_length']
#             token  = item['token']
#             value  = item['value']  
            
#             # --- skip 
#             while token in tokens_skip:
#                 i += 1
#                 if i == len(data):
#                     break
#                 item = data[i]

#                 length = item['prefix_length']
#                 token  = item['token']
#                 value  = item['value']
            
#             _items.append(item)

                
#         test.append(
#             {
#                 'token': item_ref['token'],
#                 'item_ref': item_ref,
#                 'items': _items
#             }
        
#         )
            










