import pymysql
import yaml


with open('db_connect_cfg.yaml', 'r') as file:
    db_context_dict = yaml.safe_load(file)


def db_context_manager(func):
    def wrapper(*args, **kwargs):
        try:
            with pymysql.connect(**db_context_dict) as conn:
                db_response = func(conn, *args, **kwargs)
                return db_response
        except Exception:
            raise ConnectionError('Connection failed using context: {0}'.format(db_context_dict))
        finally:
            conn.close()
    return wrapper


@db_context_manager
def db_table_read(conn, query_string):
    cursor = conn
    cursor.execute(query_string)
    data = cursor.fetchall()
    return data


def transaction(cursor, transaction_type, sql_string):
    # TODO: add columns and optional values, where, and aggregator args for SQL query string builder.
    try:
        cursor.execute(sql_string)
        cursor.commit()
        print('Commit successful.')
    except Exception:
        cursor.rollback()
        raise Exception('{0} and commit unsuccessful, rollback initiated.'.format(transaction_type))


@db_context_manager
def db_table_insert(conn, insert_string):
    cursor = conn
    transaction(cursor, 'INSERT', insert_string)


@db_context_manager
def db_table_delete(conn, delete_string):
    cursor = conn
    transaction(cursor, 'DELETE', delete_string)


@db_context_manager
def db_table_update(conn, update_string):
    cursor = conn
    transaction(cursor, 'UPDATE', update_string)


print(db_table_read('SHOW TABLES;'))
