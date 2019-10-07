import database_interface as db_api
import simplejson as json
import pprint as pp
import nest_local as nest


from datetime import date, datetime
from typing import *


def json_serial(obj: Any) -> str:
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def select_query(field_list: List[str], database: str, table: str,
                 condition_expression: Optional[str]=None,
                 aggregator_list: Optional[List[str]]=None,
                 group_by_list: Optional[List[str]]=None,
                 order_by_list: Optional[List[str]]=None,
                 order_by_direction: Optional[str]=None,
                 return_as_json: Optional[bool]=False) -> str:

    database_and_table_string = '.'.join((database, table))
    field_list_string = ','.join(field_list)

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
        group_by_string = ','.join(group_by_list)
        query_string += ' GROUP BY {}'.format(group_by_string)

    # if an order by list exists, then adds grouping hierarchy to order response by.
    if order_by_list is not None:
        order_by_string = ','.join(order_by_list)
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


def insert_statement(fields_values_dict: Dict[str, str], database: str, table: str) -> None:
    columns = fields_values_dict.keys()
    values = fields_values_dict.values()
    columns_str = ','.join(col for col in columns)
    values_str = ','.join(val for val in values)
    insert_string = 'INSERT INTO {}.{} ({}) VALUES ({});'.format(table, database, columns_str, values_str)
    db_api.db_table_insert(insert_string)


def delete_statement(database: str, table: str, condition_expression: Optional[str]=None) -> None:
    delete_string = 'DELETE FROM {}.{}'.format(table, database)
    if condition_expression is not None:
        delete_string += 'WHERE {};'
    else:
        delete_string += ';'
    db_api.db_table_delete(delete_string)


def update_statement(fields_values_dict: Dict[str, str], database: str, table: str,
                     conditional_expression: Optional[str]=None) -> None:

    update_pairs = ('{} = {}'.format(col, val) for col, val in fields_values_dict.items())
    update_pairs_string = ','.join(update_pairs)
    update_string = 'UPDATE {}.{} SET {}'.format(table, database, update_pairs_string)
    if conditional_expression is not None:
        update_string += 'WHERE {};'
    else:
        update_string += ';'
    db_api.db_table_update(update_string)


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
