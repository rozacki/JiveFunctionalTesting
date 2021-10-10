import pytest
import subprocess
import logging
import docker

from impala.dbapi import connect
from thrift.transport.TTransport import TTransportException

logging.getLogger().setLevel(logging.DEBUG)
from requests.exceptions import ConnectionError

'''
 This tests use pytest-docker on https://hshirodkar.medium.com/apache-hive-on-docker-4d7280ac6f8e
'''

def execute_cmd(cmd, output_file_path):
    try:
        sql_file = open(output_file_path, 'w')
    except Exception as ex:
        logging.error(ex)
        return False

    try:
        # capture logs
        rc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        content, err = rc.communicate()
        if err:
            logging.error(f'error while executing {cmd}')
            exit(1)

        # append to sql_file
        sql_file.write(content)

    except Exception as ex:
        logging.error(ex)
        return False

    return True


def split_sql_text(sql_text):
    sqls = []
    start = 0
    for i, v in enumerate(sql_text):
        if v == ';':
            sqls.append(sql_text[start:i].replace('\n', ' '))
            start = i+1

    return sqls


def read_sql(file_path):
    sql = ''
    with open(file_path, 'r') as f:
        for line in f:
            if not line.startswith('!'):
                sql = sql + line

    return sql


def is_responsive(host, port, auth_mechanism, user, password):
    try:
        conn = connect(host=host, port=port, auth_mechanism=auth_mechanism, user=user, password=password)
        conn.cursor()
        return True
    except TTransportException:
        return False


def create_sql(mapping, raw_hdfs_location, target_sql_path):
    if not execute_cmd(cmd=['java', '-cp', 'resources/jars/jive-service-1.0.14.jar',
                            'uk.gov.dwp.uc.dip.schemagenerator.SchemaGenerator', '-tm', mapping,
                            '-l', raw_hdfs_location], output_file_path=target_sql_path):
        return False  # need to distinguish between failed and not setup properly test

    return True


def initiate_docker_environment(docker_ip, docker_services):
    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("hive-server", 10000)

    host = docker_ip
    auth_mechanism = 'PLAIN'
    user = 'hive'
    password = 'hive'

    docker_services.wait_until_responsive(
        timeout=120.0, pause=1.0, check=lambda:
        is_responsive(host, port, auth_mechanism, user, password)
    )

    return port


def prepeare_hdfs(raw_local_path, raw_hdfs_location):
    docker_client = docker.from_env()
    hive_server_container = docker_client.containers.get('hive-server')

    # copy all test data and keep structure
    exit_code, output = hive_server_container.exec_run(f'hdfs dfs -put -f {raw_local_path} '
                                                       f'{raw_hdfs_location}')
    logging.info(f'HiveServer output {output}')
    logging.info(f'HiveServer exit code {exit_code}')
    if exit_code:
        assert False


def prepare_cursor(docker_ip, port):
    conn = connect(host=docker_ip, port=port, auth_mechanism='PLAIN', user='hive', password='hive')
    cursor = conn.cursor()
    cursor.execute(f'add jar /test_data/json-serde-1.3.8-jar-with-dependencies.jar')

    return cursor

# clean hdfs, local and hive store by dropping dirs
@pytest.fixture(scope='session')
def cursor(docker_ip, docker_services):
    raw_local_path = '/test_data'
    raw_hdfs_location = '/user/hive/'

    port = initiate_docker_environment(docker_ip, docker_services)

    prepeare_hdfs(raw_local_path, raw_hdfs_location)

    cursor = prepare_cursor(docker_ip, port)

    return cursor


class TestArraysOfArrays:
    test_name = 'arrays_of_arrays'

    @pytest.fixture(scope='class')
    def cursor(self, cursor):
        mapping = f'resources/mappings/{self.test_name}.csv'
        raw_hdfs_location = '/user/hive/'
        target_sql_path = f'tmp/{self.test_name}.sql'

        create_sql(mapping, raw_hdfs_location, target_sql_path)

        # read sql file
        sqls = split_sql_text(read_sql(target_sql_path))
        logging.info(sqls)
        for sql in sqls:
            cursor.execute(sql)

        return cursor

    def test_count(self, cursor):
        # table names is in the mapping
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array2 IS NOT NULL'

        cursor.execute(sql)

        all_rows = cursor.fetchall()

        assert all_rows[0][0] == 3

    def test_rows(self, cursor):
        sql = f'SELECT array1_array2 FROM {self.test_name} WHERE array1_array2 IS NOT NULL'
        cursor.execute(sql)
        all_rows = cursor.fetchall()

        assert 'a' == all_rows[0][0]
        assert 'b' == all_rows[1][0]
        assert 'c' == all_rows[2][0]

        assert True

    def test_nested_array_and_struct_count(self, cursor):
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array3_a IS NOT NULL'
        cursor.execute(sql)
        all_rows = cursor.fetchall()
        assert 3 == all_rows[0][0]

    def test_nested_array_and_struct_rows(self, cursor):
        sql = f"SELECT array1_array3_a,array1_array3_b FROM {self.test_name} WHERE array1_array3_a is not null and array1_array3_b IS NOT NULL"
        cursor.execute(sql)
        all_rows = cursor.fetchall()

        assert ('a1', 'b1',) == all_rows[0]
        assert ('a2', 'b2',) == all_rows[1]
        assert ('a3', 'b3',) == all_rows[2]

    def test_array_single_element(self, cursor):
        sql = f"SELECT array1_array4_array4_index FROM {self.test_name} where  array1_array4_array4_index is not null"
        cursor.execute(sql)
        all_rows = cursor.fetchall()

        assert 1 == len(all_rows)

    def test_nested_array_and_index_rows(self, cursor):
        sql = f'SELECT array1_array4_array4_index FROM  {self.test_name} where  array1_array4_array4_index is not null'
        cursor.execute(sql)
        all_rows = cursor.fetchall()

        assert 1 == all_rows[0][0]