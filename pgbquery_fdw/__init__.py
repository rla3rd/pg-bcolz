from multicorn import ForeignDataWrapper
# we'll put logging in place when i get closer to a functional wrapper
from multicorn.utils import log_to_postgres, INFO, ERROR, WARNING, DEBUG
import os
import sys
import traceback
import pgbquery

class BqueryForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        try:
            super(BqueryForeignDataWrapper, self).__init__(options, columns)
            self.options = options
            self.columns = columns
            self.rootdir = self.options.get('rootdir', os.environ['PGBQUERY_HOME'])
            self.mode = self.options.get('mode', 'rw')
            self.names = [col for col in self.columns]
            self.row_id_column = options.get('primary_key', None)
            self.filter = ""

            log_to_postgres(str(dict), INFO)
      
            # somehow below i have to convert columns values ( pg column types )
            # into the appropiate dtypes to make an empty bcolz table
            # i'll open one up from disk for now
            self.table = pgbquery.open(self.rootdir, mode=self.mode)
        except:
            errMsg = "%s: %s, rootdir: %s, mode: %s" % ( sys.exc_info()[0], traceback.format_exc(), self.rootdir, self.mode)
            log_to_postgres(errMsg, ERROR)

    def execute(self, quals, columns):
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
                boolarr = test.where_terms(filters)
                for line in test.where(boolarr, outcols=test.names):
                    yield line
            else:
                for line in test.__iter__():
                    yield line
    
    def get_rel_size(self, quals, columns):
        pass
	#filters = []
	
        # qual object is qual.field_name, qual.operator, qual.value
	#for qual in quals:
	
	#rows = self.table[[filter]].size
	#nb = self.table[[filter]].nbytes
	
	
	
