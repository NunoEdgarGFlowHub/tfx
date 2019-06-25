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
"""Generate avro file from Chicago taxi csv data."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import fastavro as av
import pandas as pd


def get_schema():
  """Returns schema for Chicago taxi dataset."""
  # Allow every column to accept null types
  fields = [
      {'name': 'company', 'type': 'string'},
      {'name': 'dropoff_census_tract', 'type': 'float'},
      {'name': 'dropoff_community_area', 'type': 'float'},
      {'name': 'dropoff_latitude', 'type': 'float'},
      {'name': 'dropoff_longitude', 'type': 'float'},
      {'name': 'fare', 'type': 'float'},
      {'name': 'payment_type', 'type': 'string'},
      {'name': 'pickup_census_tract', 'type': 'float'},
      {'name': 'pickup_community_area', 'type': 'int'},
      {'name': 'pickup_latitude', 'type': 'float'},
      {'name': 'pickup_longitude', 'type': 'float'},
      {'name': 'tips', 'type': 'float'},
      {'name': 'trip_miles', 'type': 'float'},
      {'name': 'trip_seconds', 'type': 'float'},
      {'name': 'trip_start_day', 'type': 'int'},
      {'name': 'trip_start_hour', 'type': 'int'},
      {'name': 'trip_start_month', 'type': 'int'},
      {'name': 'trip_start_timestamp', 'type': 'int'}
  ]

  for column in fields:
    column['type'] = [column['type'], 'null']

  schema = {'name': 'Chicago Taxi dataset', 'type': 'record', 'fields': fields}
  return schema


def generate_avro(src_file, output_file):
  """Generates avro file based on src file.

  Args:
    src_file: path to Chicago taxi dataset.
    output_file: output path for avro file.
  """
  df = pd.read_csv(src_file)

  # Replaces NaN's with None's for avroWriter to interpret null values
  df = df.where((pd.notnull(df)), None)
  records = df.to_dict(orient='records')

  parsed_schema = av.parse_schema(get_schema())
  with open(output_file, 'wb') as f:
    av.writer(f, parsed_schema, records)


if __name__ == '__main__':
  generate_avro('../csv/data.csv', 'data.avro')
