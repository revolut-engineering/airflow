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

import mock
import unittest

from airflow.contrib.hooks.exasol_hook import ExasolHook
from airflow.models import Connection


class TestExasolHookConn(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.connection = Connection(
            login='login',
            password='password',
            host='host',
            schema='schema',
            port='port',
        )

        self.db_hook = ExasolHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('airflow.contrib.hooks.exasol_hook.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        self.assertEqual(args, ())
        self.assertEqual(kwargs['user'], 'login')
        self.assertEqual(kwargs['password'], 'password')
        self.assertEqual(kwargs['dsn'], 'host:port')
        self.assertEqual(kwargs['schema'], 'schema')


class TestExasolHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock()
        self.conn = conn = mock.MagicMock()
        self.conn.execute.return_value = self.cur

        class SubExasolHook(ExasolHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = SubExasolHook()

    def test_get_records(self):
        sql = "SELECT * FROM DUAL"
        parameters = dict(pippo=1)
        self.db_hook.get_records(sql, parameters=parameters)
        self.conn.execute.assert_called_once_with(sql, parameters)
        self.cur.fetchall.assert_called_once()
