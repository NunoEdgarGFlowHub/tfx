# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for tfx.tools.cli.cmd.helper."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import mock
import tensorflow as tf

from tfx.tools.cli import labels
from tfx.tools.cli.handler import airflow_handler
from tfx.tools.cli.handler import handler_factory
from tfx.tools.cli.handler import kubeflow_handler


class HandlerFactoryTest(tf.test.TestCase):

  def setUp(self):
    self.flags_dict = {}

  def test_create_handler_airflow(self):
    self.flags_dict[labels.ENGINE_FLAG] = 'airflow'
    self.assertIsInstance(
        handler_factory.create_handler(self.flags_dict),
        airflow_handler.AirflowHandler)

  def test_create_handler_kubeflow(self):
    self.flags_dict[labels.ENGINE_FLAG] = 'kubeflow'
    self.assertIsInstance(
        handler_factory.create_handler(self.flags_dict),
        kubeflow_handler.KubeflowHandler)

  def test_create_handler_other(self):
    self.flags_dict[labels.ENGINE_FLAG] = 'beam'
    with self.assertRaises(Exception) as err:
      handler_factory.create_handler(self.flags_dict)
    self.assertEqual(
        str(err.exception), 'Engine {} is not supported.'.format(
            self.flags_dict[labels.ENGINE_FLAG]))

  def _MockSubprocessAirflow(self):
    return b'absl-py==0.7.1\nalembic==0.9.10\napache-airflow==1.10.3\n'

  @mock.patch('subprocess.check_output', _MockSubprocessAirflow)
  def test_detect_handler_airflow(self):
    self.flags_dict[labels.ENGINE_FLAG] = 'auto'
    self.assertIsInstance(
        handler_factory.detect_handler(self.flags_dict),
        airflow_handler.AirflowHandler)

  def _MockSubprocessNoEngine(self):
    return b'absl-py==0.7.1\nalembic==0.9.10\n'

  @mock.patch('subprocess.check_output', _MockSubprocessNoEngine)
  def test_detect_handler_missing(self):
    self.flags_dict[labels.ENGINE_FLAG] = 'auto'
    with self.assertRaises(SystemExit) as cm:
      handler_factory.detect_handler(self.flags_dict)
    self.assertEqual(
        str(cm.exception),
        'Orchestrator missing in the environment.'
        )

  def _MockSubprocessMultipleEngines(self):
    return 'absl-py==0.7.1\nadal==1.2.1\nalembic==0.9.10\napache-airflow==1.10.3\napache-beam==2.12.0\nkfp==0.1\n'

  @mock.patch('subprocess.check_output', _MockSubprocessMultipleEngines)
  def test_detect_handler_multiple(self):
    self.flags_dict[labels.ENGINE_FLAG] = 'auto'
    with self.assertRaises(SystemExit) as cm:
      handler_factory.detect_handler(self.flags_dict)
    self.assertEqual(
        str(cm.exception),
        'Multiple orchestrators found. Choose one using --engine flag.'
        )

if __name__ == '__main__':
  tf.test.main()

