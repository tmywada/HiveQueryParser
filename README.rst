MetadataGeneratorHiveQuery
======================================

.. |buildstatus|_
.. |coverage|_
.. |docs|_
.. |packageversion|_

.. docincludebegin

This module extracts metadata from Hive SQL queries. This module is designed to analyse the following use cases in Hive SQL queries:

* basic SQL commands
* temporary tables
* nested CASE statemens
* nested sub-queries

This module uses  
`sqlparse <https://github.com/andialbrecht/sqlparse>`_.

Quick Start (command line)
-----------

.. code-block:: sh

   $ python MetadataGeneratorHiveQueries.py --file_path ./data/sample.sql

```
create table schema.table as select col1,col2 from schema_source.table_source;
```

```
CREATE TABLE schema.table AS
SELECT col1,
       col2
FROM schema_source.table_source;
```

```
{'token': 'create', 'type': 'table', 'value': 'schema.table', 'metadta': {'schema_name': 'schema', 'table_name': 'table', 'table_alias': None}}
{'token': 'select', 'type': 'column', 'value': 'col1', 'metadata': {'column_name': 'col1'}}
{'token': 'select', 'type': 'column', 'value': 'col2', 'metadata': {'column_name': 'col2'}}
{'token': 'FROM', 'type': 'table', 'value': 'schema_source.table_source', 'metadata': {'schema_name': 'schema_source', 'table_name': 'table_source', 'table_alias': None}}
```
.. code-block:: python

    >>> from HiveQueryParser import ParseHiveQuery

    >>> # read SQL file
    >>> string_input = read_sql_file('./data/sample.sql')

    >>> # Split a string containing two SQL statements:
    >>> raw = 'select * from foo; select * from bar;'


    
   >>> statements = sqlparse.split(raw)
   >>> statements
   ['select * from foo;', 'select * from bar;']

   >>> # Format the first statement and print it out:
   >>> first = statements[0]
   >>> print(sqlparse.format(first, reindent=True, keyword_case='upper'))
   SELECT *
   FROM foo;

   >>> # Parsing a SQL statement:
   >>> parsed = sqlparse.parse('select * from foo')[0]
   >>> parsed.tokens
   [<DML 'select' at 0x7f22c5e15368>, <Whitespace ' ' at 0x7f22c5e153b0>, <Wildcard '*' … ]
   >>>

Links
-----

Project page
   https://github.com/tmywada/HiveQueryParser


.. |buildstatus| image:: https://github.com/andialbrecht/sqlparse/actions/workflows/python-app.yml/badge.svg
.. _buildstatus: https://github.com/andialbrecht/sqlparse/actions/workflows/python-app.yml
.. |coverage| image:: https://codecov.io/gh/andialbrecht/sqlparse/branch/master/graph/badge.svg
.. _coverage: https://codecov.io/gh/andialbrecht/sqlparse
.. |docs| image:: https://readthedocs.org/projects/sqlparse/badge/?version=latest
.. _docs: https://sqlparse.readthedocs.io/en/latest/?badge=latest
.. |packageversion| image:: https://img.shields.io/pypi/v/sqlparse?color=%2334D058&label=pypi%20package
.. _packageversion: https://pypi.org/project/sqlparse