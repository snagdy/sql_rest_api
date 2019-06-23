import database_interface as db_api
import simplejson as json
import pprint as pp
import nest_local as nest
from datetime import date, datetime


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def select_query(field_list, database, table,
                 condition_expression=None,
                 aggregator_list=None,
                 group_by_list=None,
                 order_by_list=None,
                 order_by_direction=None,
                 return_as_json=False):
    database_and_table_string = '.'.join((database, table))
    field_list_string = ', '.join(field_list)

    # constructs the basic query string with or without and aggregator function.
    if aggregator_list is None:
        query_string = 'SELECT {} FROM {}'.format(field_list_string, database_and_table_string)
    else:
        # applies aggregator functions onto fields, if they are present, else just the field.
        aggregated_field_list = ['{}({})'.format(aggregator, field)
                                 if aggregator is not None else field
                                 for aggregator, field
                                 in zip(aggregator_list, field_list)]

        field_list_string = ', '.join(aggregated_field_list)
        query_string = 'SELECT {} FROM {}'.format(field_list_string, database_and_table_string)

    if condition_expression is not None:
        query_string += ' WHERE {}'.format(condition_expression)

    # if an aggregator function is called, checks for a group by list and appends grouping hierarchy.
    if group_by_list is not None and aggregator_list is not None:
        group_by_string = ', '.join(group_by_list)
        query_string += ' GROUP BY {}'.format(group_by_string)

    # if an order by list exists, then adds grouping hierarchy to order response by.
    if order_by_list is not None:
        order_by_string = ', '.join(order_by_list)
        query_string += ' ORDER BY {}'.format(order_by_string)

    # if an order by direction is specified, then it is applied.
    if order_by_direction is not None:
        query_string += ' {}'.format(order_by_direction)

    # finally end sql string with ;.
    query_string += ';'
    print(query_string)
    if return_as_json:
        return json.dumps(db_api.db_table_read(query_string), iterable_as_array=True, default=json_serial)
    else:
        return db_api.db_table_read(query_string)


# print(select_query(['MERCHANT', 'TRANSACTED_VALUE'], 'testdb', 'daily_data', aggregator_list=[None, 'SUM'], condition_expression='TRANSACTED_VALUE >50000', group_by_list=['MERCHANT']))
# pp.pprint(select_query(['MERCHANT', 'TRANSACTED_VALUE'], 'testdb', 'daily_data', aggregator_list=[None, 'SUM'], condition_expression='TRANSACTED_VALUE >50000', group_by_list=['MERCHANT']))

# sql_query = select_query(['MERCHANT', 'TRANSACTED_VALUE'],
#                          'testdb',
#                          'daily_data',
#                          aggregator_list=[None, 'SUM'],
#                          condition_expression='TRANSACTED_VALUE >50000',
#                          group_by_list=['MERCHANT'],
#                          return_as_json=True)
#
# nesting_order = ['MERCHANT', 'SUM(TRANSACTED_VALUE)']
# input_data = sql_query
# input_json = json.loads(input_data)
#
# extra_keys = nest.get_extra_keys_list(nesting_order, input_json)
# pp.pprint(nest.regroup_json_data(input_json, nesting_order, extra_keys))

# SECOND QUERY

sql_query_2 = select_query(['COMPANY', 'MERCHANT', 'TRANSACTION_DATE', 'TRANSACTED_VALUE'],
                           'testdb',
                           'daily_data',
                           condition_expression='TRANSACTED_VALUE > 1000000',
                           return_as_json=True)

# db_api.display_results(sql_query_2)


nesting_order_2 = ['COMPANY', 'MERCHANT', 'TRANSACTION_DATE', 'TRANSACTED_VALUE']
input_data_2 = sql_query_2
input_json_2 = json.loads(input_data_2)

extra_keys_2 = nest.get_extra_keys_list(nesting_order_2, input_json_2)
pp.pprint(nest.regroup_json_data(input_json_2, nesting_order_2, extra_keys_2))
