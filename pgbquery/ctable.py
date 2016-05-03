import bquery
import numpy as np
import pandas as pd
from operatorFunctions import opMap, getOperatorFunction
   
class ctable(bquery.ctable):
    def where_terms(self, term_list):
        """
        TEMPORARY WORKAROUND TILL NUMEXPR WORKS WITH IN
        where_terms(term_list, outcols=None, limit=None, skip=0)

        Iterate over rows where `term_list` is true.
        A terms list has a [(col, operator, value), ..] construction.
        Eg. [('sales', '>', 2), ('state', 'in', ['IL', 'AR'])]

        :param term_list:
        :param outcols:
        :param limit:
        :param skip:
        :return: :raise ValueError:
        """

        if type(term_list) not in [list, set, tuple]:
            raise ValueError("Only term lists are supported")

        eval_string = ''
        eval_list = []

        for term in term_list:
            filter_col = term[0]
            filter_operator = term[1].lower()
            filter_value = term[2]

            # convert postgres eq and ne to python equivalents
            if filter_operator == '=':
                filter_operator = '=='
            elif filter_operator == '<>':
                filter_operator = '!='

            if filter_operator not in ['in', 'not in'] and filter_operator not in opMap:
                # direct filters should be added to the eval_string

                # add and logic if not the first term
                if eval_string:
                    eval_string += ' & '

                eval_string += '(' + filter_col + ' ' \
                               + filter_operator + ' ' \
                               + str(filter_value) + ')'
                        
                print eval_string

            elif filter_operator in opMap:
                eval_list.append(
                    (filter_col, filter_operator, filter_value)
                )

            elif filter_operator in ['in', 'not in']:
                # Check input
                if type(filter_value) not in [list, set, tuple]:
                    raise ValueError("In selections need lists, sets or tuples")

                if len(filter_value) < 1:
                    raise ValueError("A value list needs to have values")

                elif len(filter_value) == 1:
                    # handle as eval
                    # add and logic if not the first term
                    if eval_string:
                        eval_string += ' & '

                    if filter_operator == 'not in':
                        filter_operator = '!='
                    else:
                        filter_operator = '=='

                    eval_string += '(' + filter_col + ' ' + \
                                   filter_operator

                    filter_value = filter_value[0]

                    if type(filter_value) == str:
                        filter_value = '"' + filter_value + '"'
                    else:
                        filter_value = str(filter_value)

                    eval_string += filter_value + ') '

                else:

                    if type(filter_value) in [list, tuple]:
                        filter_value = set(filter_value)

                    eval_list.append(
                        (filter_col, filter_operator, filter_value)
                    )
            else:
                raise ValueError(
                    "Input not correctly formatted for eval or list filtering"
                )

        # (1) Evaluate terms in eval
        # return eval_string, eval_list
        if eval_string:
            boolarr = self.eval(eval_string)
            if eval_list:
                # convert to numpy array for array_is_in
                boolarr = boolarr[:]
        else:
            boolarr = np.ones(self.size, dtype=bool)

        # (2) Evaluate other terms like 'in' or 'not in' ...
        for term in eval_list:

            name = term[0]
            col = self.cols[name]

            operator = term[1]
                        
            if operator.lower() in ['not in', 'in']:
                if operator.lower() == 'not in':
                    reverse = True
                elif operator.lower() == 'in':
                    reverse = False
                else:
                    raise ValueError(
                        "Input not correctly formatted for list filtering"
                    )

                value_set = set(term[2])

                ctable_ext.carray_is_in(col, value_set, boolarr, reverse)

            elif operator in opMap:
                opFunc = getOperatorFunction(operator)
                value = term[2]
                i = 0
                for row in col.iter():
                    if not opFunc(name, value):
                        boolarr[i] = False
                    i += 1

        if eval_list:
            # convert boolarr back to carray
            boolarr = bquery.carray(boolarr)

        return boolarr

