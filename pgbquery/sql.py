"""
Collection of query wrappers / abstractions to both facilitate data
retrieval and to reduce dependency on DB-specific API.
Originally based on Pandas.io.sql.sql_legacy module, and converted to support ctables
Pandas has since moved on to using sqlalchemy as its sql engine
Since were only concerned with Postgresql this should be good enough for now
"""
from __future__ import print_function
from datetime import datetime
import itertools
import numpy as np
import sys
import traceback
import cStringIO
import csv
from pgbquery import ctable

PY2 = sys.version_info[0] == 2
PY3 = (sys.version_info[0] >= 3)

if PY3:
    string_types = str
if PY2:
    string_types = basestring

range = xrange
lzip =zip
map = itertools.imap
zip = itertools.izip


#------------------------------------------------------------------------------
# Helper execution function


def execute(sql, con, retry=True, cur=None, params=None):
    """
    Execute the given SQL query using the provided connection object.

    Parameters
    ----------
    sql: string
        Query to be executed
    con: database connection instance
        Database connection.  Must implement PEP249 (Database API v2.0).
    retry: bool
        Not currently implemented
    cur: database cursor, optional
        Must implement PEP249 (Datbase API v2.0).  If cursor is not provided,
        one will be obtained from the database connection.
    params: list or tuple, optional
        List of parameters to pass to execute method.

    Returns
    -------
    Cursor object
    """
    try:
        if cur is None:
            cur = con.cursor()

        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur
    except Exception:
        try:
            con.rollback()
        except Exception:  # pragma: no cover
            pass

        print('Error on sql %s' % sql)
        raise


def _safe_fetch(cur):
    try:
        result = cur.fetchall()
        if not isinstance(result, list):
            result = list(result)
        return result
    except Exception as e:  # pragma: no cover
        excName = e.__class__.__name__
        if excName == 'OperationalError':
            return []


def tquery(sql, con=None, cur=None, retry=True):
    """
    Returns list of tuples corresponding to each row in given sql
    query.

    If only one column selected, then plain list is returned.

    Parameters
    ----------
    sql: string
        SQL query to be executed
    con: SQLConnection or DB API 2.0-compliant connection
    cur: DB API 2.0 cursor

    Provide a specific connection or a specific cursor if you are executing a
    lot of sequential statements and want to commit outside.
    """
    cur = execute(sql, con, cur=cur)
    result = _safe_fetch(cur)

    if con is not None:
        try:
            cur.close()
            con.commit()
        except Exception as e:
            excName = e.__class__.__name__
            if excName == 'OperationalError':  # pragma: no cover
                print('Failed to commit, may need to restart interpreter')
            else:
                raise

            traceback.print_exc()
            if retry:
                return tquery(sql, con=con, retry=False)

    if result and len(result[0]) == 1:
        # python 3 compat
        result = list(lzip(*result)[0])
    elif result is None:  # pragma: no cover
        result = []

    return result


def uquery(sql, con=None, cur=None, retry=True, params=None):
    """
    Does the same thing as tquery, but instead of returning results, it
    returns the number of rows affected.  Good for update queries.
    """
    cur = execute(sql, con, cur=cur, retry=retry, params=params)

    result = cur.rowcount
    try:
        con.commit()
    except Exception as e:
        excName = e.__class__.__name__
        if excName != 'OperationalError':
            raise

        traceback.print_exc()
        if retry:
            print('Looks like your connection failed, reconnecting...')
            return uquery(sql, con, retry=False)
    return result


def read_ctable(sql, con, index_col=None, coerce_float=True, params=None):
    """
    Returns a DataFrame corresponding to the result set of the query
    string.

    Optionally provide an index_col parameter to use one of the
    columns as the index. Otherwise will be 0 to len(results) - 1.

    Parameters
    ----------
    sql: string
        SQL query to be executed
    con: DB connection object, optional
    index_col: string, optional
        column name to use for the returned DataFrame object.
    coerce_float : boolean, default True
        Attempt to convert values to non-string, non-numeric objects (like
        decimal.Decimal) to floating point, useful for SQL result sets
    params: list or tuple, optional
        List of parameters to pass to execute method.
    """
    cur = execute(sql, con, params=params)
    rows = _safe_fetch(cur)
    columns = [col_desc[0] for col_desc in cur.description]

    cur.close()
    con.commit()

    result = DataFrame.from_records(rows, columns=columns,
                                    coerce_float=coerce_float)

    if index_col is not None:
        result = result.set_index(index_col)

    return result

ctable_query = read_ctable
read_sql = read_ctable


def write_ctable(ctable, name, con, if_exists='fail', **kwargs):
    """
    Write records stored in a DataFrame to a SQL database.

    Parameters
    ----------
    ctable: Pgbquery ctable
    name: name of SQL table
    con: an open SQL database connection object
    if_exists: {'fail', 'replace', 'append'}, default 'fail'
        fail: If table exists, do nothing.
        replace: If table exists, drop it, recreate it, and insert data.
        append: If table exists, insert data. Create if does not exist.
    """

    if 'append' in kwargs:
        import warnings
        warnings.warn("append is deprecated, use if_exists instead",
                      FutureWarning)
        if kwargs['append']:
            if_exists = 'append'
        else:
            if_exists = 'fail'
    exists = table_exists(name, con)
    if if_exists == 'fail' and exists:
        raise ValueError("Table '%s' already exists." % name)

    #create or drop-recreate if necessary
    create = None
    if exists and if_exists == 'replace':
        create = "DROP FOREIGN TABLE %s; %s" % (name, get_schema(ctable, name))
    elif not exists:
        create = get_schema(ctable, name)

    if create is not None:
        cur = con.cursor()
        cur.execute(create)
        cur.close()

    cur = con.cursor()
    # Replace spaces in ctable column names with _.
    safe_names = [s.replace(' ', '_').strip() for s in ctable.names]
    _write_postgres(ctable, name, safe_names, cur)
    cur.close()
    con.commit()

def _write_postgres(ctable, table, names, cur):
    output = cStringIO.StringIO()
    writer = csv.writer(output, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
   
    for col in ctable.dtypes.index:
        dt =ctable.dtypes[col]
        if str(dt.type)=="<type 'numpy.object_'>":
            ctable[col] = ctable[col].apply(lambda x: x.__str__().replace('\n', ''))

    [writer.writerow(tuple(x)) for x in ctable.__iter__()]
    
    output.seek(0)
    cur.copy_from(output, table, columns=names)    

def table_exists(name, con):
    schema = None
    if '.' in name:
        (schema, name) = name.split('.')
    else:
        schema = 'public'
    query = "select tablename FROM pg_tables WHERE schemaname = '%s' and tablename='%s'" % (schema, name)
    return len(tquery(query, con)) > 0


def get_sqltype(pytype):
    sqltype = "CHARACTER VARYING" 

    if issubclass(pytype, np.floating):
        sqltype = 'NUMERIC'

    if issubclass(pytype, np.integer):
        #TODO: Refine integer size.
        sqltype = 'BIGINT'

    if issubclass(pytype, np.datetime64) or pytype is datetime:
        # Caution: np.datetime64 is also a subclass of np.number.
        sqltype = 'TIMESTAMP'

    if pytype is datetime.date:
        sqltype = 'TIMESTAMP'

    if issubclass(pytype, np.bool_):
        sqltype = 'BOOLEAN'

    return sqltype


def get_schema(ctable, name, keys=None):
    "Return a CREATE FOREIGN TABLE statement to suit the contents of a ctable."
    lookup_type = lambda dtype: get_sqltype(dtype.type)
    # Replace spaces in DataFrame column names with _.
    # Also force lowercase, postgresql can be case sensitive
    safe_columns = [s.replace(' ', '_').strip().lower() for s in ctable.dtypes.index]
    column_types = lzip(safe_columns, map(lookup_type, ctable.dtypes))
    columns = ',\n  '.join('"%s" %s' % x for x in column_types)

    keystr = ''
    if keys is not None:
        if isinstance(keys, string_types):
            keys = (keys,)
        keystr = ', PRIMARY KEY (%s)' % ','.join(keys)
    template = """CREATE FOREIGN TABLE %(name)s (
                  %(columns)s
                  %(keystr)s
                  );"""
    create_statement = template % {'name': name, 'columns': columns,
                                   'keystr': keystr}
    return create_statement
