import os
from hive import execute_cmd, read_sql, split_sql_text, create_sql


def test_split_sql_text():
    sqls = split_sql_text('select * from employees;'
                   ' '
                   'select * from items'
                   ';')

    print(sqls)
    assert len(sqls) == 2


def test_split_sql_text_from_file():
    sql_text = read_sql('tests/resources/sql/arrays_of_arrays.sql')
    sqls = split_sql_text(sql_text)

    print(sqls)
    assert len(sqls) == 4


def test_read_sql():
    sql = read_sql('tests/resources/sql/arrays_of_arrays.sql')
    assert sql


def test_generate_sql():
    mapping = 'tests/resources/mapping/arrays_of_arrays.csv'
    raw_hdfs_location = '/user/hive'
    target_sql_path = 'tests/tmp/test_generate_sql.sql'
    if not create_sql(mapping=mapping, raw_hdfs_location=raw_hdfs_location, target_sql_path=target_sql_path):
        assert False

    file_size = os.path.getsize(target_sql_path)

    if file_size == 0:
        assert False

    assert True