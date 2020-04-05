import mysql.connector
from pprint import pprint


class DbConnector:
    __connector = None

    def __init__(self):
        pass

    @classmethod
    def get_instance(cls, host, user, password, db_name):
        if not cls.__connector:
            cls.__connector = mysql.connector.connect(
                user=user,
                password=password,
                host=host,
                database=db_name
            )

            return cls.__connector
        else:
            return cls.__connector

    @classmethod
    def query(cls, query_string, attr):
        try:
            cursor = cls.__connector.cursor(dictionary=True)
            cursor.execute(query_string)
            if 'debug' in attr and attr['debug'] > 0:
                print(query_string + "\n")

            if 'SELECT' in query_string.upper():
                return cursor.fetchall()

            if 'UPDATE' in query_string.upper():
                cls.__connector.commit()
                return {'UPDATED_ROWS': cursor.rowcount}

            if 'DELETE' in query_string.upper():
                cls.__connector.commit()
                return {'DELETED_ROWS': cursor.rowcount}

            if 'INSERT' in query_string.upper():
                cls.__connector.commit()
                return {'INSERT_ID': cursor.lastrowid}

        except mysql.connector.Error as err:
            return {'Error': err}

    @classmethod
    def search_fields(cls, enable_elements, attr):
        result = {
            'WHERE': '',
            'WHERE_WITHOUT_PREFIX': '',
            'SHOW': '',
            'SET': ''
        }
        if 'WHERE' in attr:
            for key in attr['WHERE']:
                if key not in enable_elements or 'TABLE_VIEW' not in enable_elements[key]:
                    continue
                set_value = '"' + str(attr['WHERE'][key]) + '"' if enable_elements[key]['TYPE'] == 'STR' \
                    else str(attr['WHERE'][key])

                where_value = enable_elements[key]['TABLE_VIEW'] + '=' + set_value
                result['WHERE'] += ' AND ' + where_value if result['WHERE'] else 'WHERE ' + where_value

                where_value = str(key).lower() + '=' + set_value
                result['WHERE_WITHOUT_PREFIX'] += ' AND ' + where_value if result['WHERE_WITHOUT_PREFIX'] \
                    else 'WHERE ' + where_value

        if 'SHOW' in attr:
            for key in attr['SHOW']:
                if key not in enable_elements or 'TABLE_VIEW' not in enable_elements[key]:
                    continue
                select_field = enable_elements[key]['TABLE_VIEW']
                result['SHOW'] += ',' + select_field if result['SHOW'] else select_field

        if 'SET' in attr:
            for key in attr['SET']:
                if key not in enable_elements or 'TABLE_VIEW' not in enable_elements[key]:
                    continue
                set_value = '"' + str(attr['SET'][key]) + '"' if enable_elements[key]['TYPE'] == 'STR' \
                    else str(attr['SET'][key])
                where_value = enable_elements[key]['TABLE_VIEW'] + '=' + set_value
                result['SET'] += ',' + where_value if result['SET'] else 'SET ' + where_value

        return result

    @classmethod
    def insert(cls, attr):
        columns = ''
        values = ''
        for column in attr['COLUMNS']:
            columns += ',' + column.lower() if columns else column.lower()
            values += ',"' + str(attr['COLUMNS'][column]) + '"' if values else '"' + str(attr['COLUMNS'][column]) + '"'

        query = 'INSERT INTO ' + attr['TABLE'] + '(' + columns + ') VALUES (' + values + ')'
        return cls.query(query, attr)

