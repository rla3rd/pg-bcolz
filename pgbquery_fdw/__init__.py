from multicorn import ForeignDataWrapper
# we'll put logging in place when i get closer to a functional wrapper
from multicorn.utils import log_to_postgres, INFO, ERROR, WARNING, DEBUG
import os
import sys
import itertools
import traceback
import pgbquery

def length_hint(obj, default=0):
    try:
        return len(obj)
    except TypeError:
        try:
            get_hint = type(obj).__length_hint__
        except AttributeError:
            return default
        try:
            hint = get_hint(obj)
        except TypeError:
            return default
        if hint is NotImplemented:
            return default
        if not isinstance(hint, int):
            raise TypeError("Length hint must be an integer, not %r" %
                            type(hint))
        if hint < 0:
            raise ValueError("__length_hint__() should return >= 0")
        return hint

class PgbqueryWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        try:
            super(PgbqueryWrapper, self).__init__(options, columns)
	    self.options = options
            self.columns = columns
            self.rootdir = self.options.get('rootdir', os.environ['PGBQUERY_HOME'])
            self.mode = self.options.get('mode', 'rw')
            self.names = [col for col in self.columns]
            self.row_id_column = self.options.get('primary_key', None)
            self.filter = ""

            log_to_postgres(str(dict), INFO)
      
            # somehow below i have to convert columns values ( pg column types )
            # into the appropiate dtypes to make an empty bcolz table
            # i'll open one up from disk for now
            self.ctable = pgbquery.open(self.rootdir, mode=self.mode)
        except:
            errMsg = "%s: %s, rootdir: %s, mode: %s" % ( sys.exc_info()[0], traceback.format_exc(), self.rootdir, self.mode)
            log_to_postgres(errMsg, ERROR)

    def _where(self, quals):
        filters = []
        for qual in quals:

            if not qual.is_list_operator:
                if type(qual.value) == str:
                    value = "'%s'" % qual.value
                else:
                    value = qual.value
                filters.append((str(qual.field_name), str(qual.operator), str(value)))
            else:
                ANY = object()
                ALL = object() 
                op = qual.operator[0]
                optype = qual.operator[1]
                if op in ('=', '==') and optype == ANY:
                    operator = 'IN'
                if op in ('!=', '<>') and optype == ANY:
                    operator = 'NOT IN'
                    filters.append((qual.fieldname, operator, qual.value))

            if len(quals) > 0:
                boolarr = self.ctable.where_terms(filters)
                return self.ctable.where(boolarr, outcols=test.names)
            else:
                return self.ctable.__iter__()

    def execute(self, quals, columns):
	rows = self._where(quals)
	for row in rows:
            yield row
""" 
    def get_rel_size(self, quals, columns):
	rows = self._where(quals)
	estRows = length_hint(rows)
	row = itertools.islice(rows, 1)
	estWidth = estRows * sys.getsizeof(row)
	return (estRows, estWidth)
"""
