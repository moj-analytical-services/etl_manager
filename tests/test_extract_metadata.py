import cx_Oracle

from etl_manager.extract_metadata import get_table_names


class TestConnection(cx_Oracle.Connection):
    def cursor(self, scrollable=False):
        return TestCursor(self, scrollable)


class TestCursor(cx_Oracle.Cursor):
    def execute(self, sql):
        result = super(TestConnection, self).execute(sql)
        print("Test")
        return result


def test_get_table_names():
    get_table_names("DB", TestConnection())


def test_create_database_json():
    return


def test_create_table_json():
    return


def test_get_table_meta():
    return


def test_get_primary_key_fields():
    return


def test_get_partitions():
    return


def test_get_subpartitions():
    return
