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
# create multicorn server wrapper
def createMulticornServer(con):
    sql = """
	CREATE SERVER pgbquery_srv
   	FOREIGN DATA WRAPPER multicorn
	  OPTIONS (wrapper 'pgbquery_fdw.PgbqueryWrapper');
	"""
    cursor = con.cursor()
    cursor.execute(sql)

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


def createForeignTable(con, ctable, name, options, keys=None):
    cursor = con.cursor()

    "Return a CREATE FOREIGN TABLE statement to suit the contents of a ctable."
    lookup_type = lambda dtype: get_sqltype(dtype.type)

    # Replace spaces in DataFrame column names with _.
    # Also force lowercase, postgresql can be case sensitive
    safe_columns = [s.replace(' ', '_').strip().lower() for s in ctable.dtypes.index]
    column_types = lzip(safe_columns, map(lookup_type, ctable.dtypes))
    columns = ',\n  '.join('"%s" %s' % x for x in column_types)

    template = """CREATE FOREIGN TABLE %(name)s (
                  	%(columns)s
                  ) SERVER pgbquery_srv
		  OPTIONS(primary_key %(pk)s, rootdir %(root)s, mode %(mode)s);
		"""
    sql = template % {'name': name, 'columns': columns, 'pk': options['primary_key'], 'rootdir': options['rootdir'], 'mode': options['mode']}
    cursor.execute(sql)
