import pytest
import subprocess
import logging
import docker


from impala.dbapi import connect
from thrift.transport.TTransport import TTransportException

logging.getLogger().setLevel(logging.DEBUG)

'''
 This tests use pytest-docker on https://hshirodkar.medium.com/apache-hive-on-docker-4d7280ac6f8e
'''


def prepare_database_cursor(test_name, cursor):
    mapping = f'resources/mappings/{test_name}.csv'
    raw_hdfs_location = '/user/hive/'
    target_sql_path = f'tmp/{test_name}.sql'

    create_sql(mapping, raw_hdfs_location, target_sql_path)

    # read sql file
    sqls = split_sql_text(read_sql(target_sql_path))
    if not len(sqls):
        assert False
    logging.info(sqls)
    for sql in sqls:
        cursor.execute(sql)

    return cursor


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


def prepare_hdfs(raw_local_path, raw_hdfs_location):
    docker_client = docker.from_env()
    hive_server_container = docker_client.containers.get('hive-server')

    # copy all test data and keep structure
    exit_code, output = hive_server_container.exec_run(f'hdfs dfs -put -f {raw_local_path} '
                                                       f'{raw_hdfs_location}')
    logging.info(f'HiveServer output {output}')
    logging.info(f'HiveServer exit code {exit_code}')
    if exit_code:
        assert False

    return True


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

    if not prepare_hdfs(raw_local_path, raw_hdfs_location):
        return False

    cursor = prepare_cursor(docker_ip, port)

    return cursor