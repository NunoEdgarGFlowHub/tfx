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
"""TFX ExampleGen component definition."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Optional
from typing import Text

from tfx.components.base import base_component
from tfx.components.base import base_executor
from tfx.components.base.base_component import ChannelParameter
from tfx.components.base.base_component import ExecutionParameter
from tfx.components.example_gen import driver
from tfx.components.example_gen import utils
from tfx.proto import example_gen_pb2
from tfx.utils import channel
from tfx.utils import types


class ExampleGenSpec(base_component.ComponentSpec):
  """ExampleGen component spec."""

  COMPONENT_NAME = 'ExampleGen'
  PARAMETERS = {
      'input_config': ExecutionParameter(type=example_gen_pb2.Input),
      'output_config': ExecutionParameter(type=example_gen_pb2.Output),
  }
  INPUTS = {}
  OUTPUTS = {
      'examples': ChannelParameter(type_name='ExamplesPath'),
  }


class FileBasedExampleGenSpec(base_component.ComponentSpec):
  """File-based ExampleGen component spec."""

  COMPONENT_NAME = 'ExampleGen'
  PARAMETERS = {
      'input_config': ExecutionParameter(type=example_gen_pb2.Input),
      'output_config': ExecutionParameter(type=example_gen_pb2.Output),
  }
  INPUTS = {
      'input_base': ChannelParameter(type_name='ExternalPath'),
  }
  OUTPUTS = {
      'examples': ChannelParameter(type_name='ExamplesPath'),
  }


class _ExampleGen(base_component.BaseComponent):
  """Official TFX ExampleGen component base class.

  ExampleGen component takes input data source, and generates train
  and eval example splits (or custom splits) for downsteam components.
  """

  SPEC_CLASS = ExampleGenSpec
  # EXECUTOR_CLASS should be overridden by subclasses.
  EXECUTOR_CLASS = base_executor.BaseExecutor

  def __init__(self,
               input_config: example_gen_pb2.Input,
               output_config: Optional[example_gen_pb2.Output] = None,
               component_name: Optional[Text] = 'ExampleGen',
               example_artifacts: Optional[channel.Channel] = None,
               name: Optional[Text] = None):
    """Construct an ExampleGen component.

    Args:
      input_config: An example_gen_pb2.Input instance, providing input
        configuration.
      output_config: An example_gen_pb2.Output instance, providing output
        configuration. If unset, default splits will be 'train' and 'eval' with
        size 2:1.
      component_name: Name of the component, should be unique per component
        class. Default to 'ExampleGen', can be overwritten by sub-classes.
      example_artifacts: Optional channel of 'ExamplesPath' for output train and
        eval examples.
      name: Unique name for every component class instance.
    """
    # Configure inputs and outputs.
    input_config = input_config or utils.make_default_input_config()
    output_config = output_config or utils.make_default_output_config(
        input_config)
    example_artifacts = example_artifacts or channel.as_channel(
        [types.TfxArtifact('ExamplesPath', split=split_name)
         for split_name in utils.generate_output_split_names(
             input_config, output_config)])
    spec = ExampleGenSpec(
        component_name=component_name,
        input_config=input_config,
        output_config=output_config,
        examples=example_artifacts)
    super(_ExampleGen, self).__init__(spec=spec, name=name)


class _FileBasedExampleGen(base_component.BaseComponent):
  """TFX file-based ExampleGen component base class.

  ExampleGen component takes input data source, and generates train
  and eval example splits (or custom splits) for downsteam components.
  """

  SPEC_CLASS = FileBasedExampleGenSpec
  # EXECUTOR_CLASS should be overridden by subclasses.
  EXECUTOR_CLASS = base_executor.BaseExecutor
  DRIVER_CLASS = driver.Driver

  def __init__(self,
               input_base: channel.Channel,
               input_config: Optional[example_gen_pb2.Input] = None,
               output_config: Optional[example_gen_pb2.Output] = None,
               component_name: Optional[Text] = 'ExampleGen',
               example_artifacts: Optional[channel.Channel] = None,
               name: Optional[Text] = None):
    """Construct a FileBasedExampleGen component.

    Args:
      input_base: A Channel of 'ExternalPath' type, which includes one artifact
        whose uri is an external directory with data files inside.
      input_config: An optional example_gen_pb2.Input instance, providing input
        configuration. If unset, the files under input_base (must set) will be
        treated as a single split.
      output_config: An optional example_gen_pb2.Output instance, providing
        output configuration. If unset, default splits will be 'train' and
        'eval' with size 2:1.
      component_name: Name of the component, should be unique per component
        class. Default to 'ExampleGen', can be overwritten by sub-classes.
      example_artifacts: Optional channel of 'ExamplesPath' for output train and
        eval examples.
      name: Unique name for every component class instance.
    """
    # Configure inputs and outputs.
    input_config = input_config or utils.make_default_input_config()
    output_config = output_config or utils.make_default_output_config(
        input_config)
    example_artifacts = example_artifacts or channel.as_channel(
        [types.TfxArtifact('ExamplesPath', split=split_name)
         for split_name in utils.generate_output_split_names(
             input_config, output_config)])
    spec = FileBasedExampleGenSpec(
        component_name=component_name,
        input_base=input_base,
        input_config=input_config,
        output_config=output_config,
        examples=example_artifacts)
    super(_FileBasedExampleGen, self).__init__(spec=spec, name=name)
