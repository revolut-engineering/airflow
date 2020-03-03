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
#


from contextlib import closing

from pyexasol import connect

from airflow.hooks.dbapi_hook import DbApiHook


class ExasolHook(DbApiHook):
    """
    Interact with Vertica.
    """

    conn_name_attr = 'exasol_conn_id'
    default_conn_name = 'exasol_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns exasol connection object
        """
        conn = self.get_connection(self.exasol_conn_id)
        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": conn.schema,
            "dsn": (conn.host or 'localhost') + ':' + str(conn.port)
        }

        conn = connect(**conn_config)
        return conn

    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        with closing(self.get_conn()) as conn:
            if parameters is not None:
                cur = conn.execute(sql, parameters)
            else:
                cur = conn.execute(sql)
            return cur.fetchall()
