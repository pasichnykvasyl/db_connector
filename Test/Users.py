import db_connector

dbCon = db_connector.DbConnector()
dbCon.get_instance('localhost', 'abills', 'sqlpassword', 'pasichnyk')


class Users:
    _db_connector = None

    def __init__(self, _connector):
        if not self._db_connector:
            self._db_connector = _connector

    def info(self, attr):
        search_fields = self._get_search_fields(attr)
        query = "SELECT " + search_fields['SHOW'] + " FROM users u " + search_fields['WHERE']
        return self._db_connector.query(query, attr)

    def update(self, attr):
        search_fields = self._get_search_fields(attr)
        query = "UPDATE users u " + search_fields['SET'] + " " + search_fields['WHERE']
        return self._db_connector.query(query, attr)

    def delete(self, attr):
        search_fields = self._get_search_fields(attr)
        query = "DELETE FROM users " + search_fields['WHERE_WITHOUT_PREFIX']
        return self._db_connector.query(query, attr)

    def insert(self, attr):
        return self._db_connector.insert({'TABLE': 'users', 'COLUMNS': attr})

    def _get_search_fields(self, attr):
        return self._db_connector.search_fields({
            'ID': {'TYPE': 'INT', 'TABLE_VIEW': 'u.id', 'AS': 'user_id'},
            'NAME': {'TYPE': 'STR', 'TABLE_VIEW': 'u.name'},
            'AGE': {'TYPE': 'INT', 'TABLE_VIEW': 'u.age'},
        }, attr)


users = Users(dbCon)
users_list = users.info({'SHOW': ['ID', 'NAME', 'AGE']})
print(users_list)

print(users.delete({'WHERE': {'NAME': 'new_test'}}))

users_list = users.info({'SHOW': ['ID', 'NAME', 'AGE']})
print(users_list)
