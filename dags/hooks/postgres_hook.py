# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import psycopg2
import psycopg2.extensions
from contextlib import closing
from airflow.hooks.base import BaseHook

#from airflow.hooks.dbapi_hook import DbApiHook


class PostgresHook:
    """
    Interact with Postgres.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.
    """
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def __init__(self, postgres_conn_id=None, schema=None):
        """
        Initialize the PostgresHook.
        
        :param postgres_conn_id: The connection ID to use
        :type postgres_conn_id: str
        :param schema: The schema to use
        :type schema: str
        """
        self.postgres_conn_id = postgres_conn_id or self.default_conn_name
        self.schema = schema
        self.conn = None

    def get_connection(self, conn_id=None):
        """
        Get the connection from Airflow connections.
        
        :param conn_id: The connection ID to use
        :type conn_id: str
        :return: The connection object
        :rtype: airflow.models.connection.Connection
        """
        conn_id = conn_id or self.postgres_conn_id
        return BaseHook.get_connection(conn_id)

    def get_conn(self):
        """
        Get a PostgreSQL connection.
        
        :return: The PostgreSQL connection
        :rtype: psycopg2.extensions.connection
        """
        if self.conn is None:
            conn = self.get_connection(self.postgres_conn_id)
            conn_args = dict(
                host=conn.host,
                user=conn.login,
                password=conn.password,
                dbname=self.schema or conn.schema,
                port=conn.port)
            
            # check for ssl parameters in conn.extra
            for arg_name, arg_val in conn.extra_dejson.items():
                if arg_name in ['sslmode', 'sslcert', 'sslkey',
                              'sslrootcert', 'sslcrl', 'application_name',
                              'keepalives_idle']:
                    conn_args[arg_name] = arg_val

            self.conn = psycopg2.connect(**conn_args)
        
        return self.conn

    def copy_expert(self, sql, filename, open=open):
        """
        Executes SQL using psycopg2 copy_expert method.
        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        if not os.path.isfile(filename):
            with open(filename, 'w'):
                pass

        with open(filename, 'r+') as f:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, f)
                    f.truncate(f.tell())
                    conn.commit()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        self.copy_expert("COPY {table} FROM STDIN".format(table=table), tmp_file)

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        self.copy_expert("COPY {table} TO STDOUT".format(table=table), tmp_file)

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell