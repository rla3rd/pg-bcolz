from multicorn import ForeignDataWrapper
import bcolz as bz
import operator as op

operators = {
    '>' : op.gt,
    '>=': op.ge,
    '<' : op.lt,
    '<=': op.le,
    '=' : op.eq,
    '<>' : op.ne,
    '~~': sqlops.like_op,
    '~~*': sqlops.ilike_op,
    '!~~*': not_(sqlops.ilike_op),
    '!~~': not_(sqlops.like_op),
    ('=', True): sqlops.in_op,
    ('<>', False): not_(sqlops.in_op)
}    
    


class BcolzForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(BcolzForeignDataWrapper, self).__init__(options, columns)
        self.table = 
        self.rowid_column = options.get('primary_key', None)
        self.columns = columns
        
        

    def execute(self, qual):
        for idx in xrange(self.table.__len__()):
            line = {}
            for name in self.table.names:
                line[name] = self.table[name][idx]
            yield line
        
    def insert(self, newVals):
        pass

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



