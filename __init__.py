from multicorn import ForeignDataWrapper
# we'll put logging in place when i get closer to a functional wrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
import bcolz as bz
from operatorFunctions import unknownOperatorException, getOperatorFunction
import re
import os

class BcolzForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(BcolzForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.rootdir = options.get('rootdir', os.environ['BCOLZ_HOME'])
        self.mode = options.get('mode', 'rw')
        self.names = [col for col in self.columns]
        self.row_id_column = options.get('primary_key', None)
        
        # somehow below i have to convert columns values ( pg column types )
        # into the appropiate dtypes to make an empty bcolz table
        self.table = bz.ctables(open(self.rootdir, mode=self.mode)
        sqlacols = []
       

    def execute(self, quals, columns):
         for qual in quals:

            try:
                operatorFunction = getOperatorFunction(qual.operator)
            except unknownOperatorException, e:
                log_to_postgres(e, ERROR)

            # now what do we do now here to filter?
            # aah i know these likely have to go into self.table[["myfoofilters"]] 
            # then loop thru the xrange results below
                                
            qual.field_name, qual.value))

        for idx in xrange(self.table.__len__()):
            line = {}
            for name in self.columns:
                line[name] = self.table[name][idx]
            yield line
        
    def insert(self, newVals):
        line = {}
        for name in self.columns:
            line[name] = 

    def update(self, oldVals, newVals):
        pass

    def delete(self, oldVals):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def pre_commit(self):
        pass

    def get_rel_size(self):
        pass

    def get_path_keys(self):
        pass



