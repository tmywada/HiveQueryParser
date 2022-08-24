HiveQueryParser
======================================

.. |buildstatus|_
.. |coverage|_
.. |docs|_
.. |packageversion|_

.. docincludebegin

This module extracts metadata of SQL queries including nested sub-queries. 





Notes
-----------
This module is designed to cover the following items in Hive SQL queries:

* basic SQL commands
* nested CASE statement
* temporary table
* nested sub-queries (i.e., sub-query inside of a subquery)

HiveQueryParser uses 
`sqlparse <https://github.com/andialbrecht/sqlparse>`_.

Quick Start
-----------


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