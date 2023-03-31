#!/usr/bin/env bash
# Copyright 2023 Greenmask
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


function exit0() {
 exit 0
}

trap 'exit0' 15

transform=false
validate=false
print_config=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --meta)
      metadata="$2"
      shift # past argument
      shift # past value
      ;;
    --print-definition)
      print_config=true
      shift # past argument
      ;;
    --validate)
      validate=true
      shift # past argument
      ;;
    --transform)
      transform=true
      shift # past argument
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

if [ $print_config = "true" ]; then
  echo '{"name":"TwoDatesGen","description":"Generate diff between two dates","parameters":[{"name":"column_a","description":"test1","required":true,"is_column":true,"column_properties":{"affected":true,"allowed_types":["date","timestamp","timestamptz"]}},{"name":"column_b","description":"test2","required":true,"is_column":true,"column_properties":{"affected":true,"allowed_types":["date","timestamp","timestamptz"]}}]}'
  exit 0
elif [ $transform = "true" ]; then
    cat
elif [ $validate = "true" ]; then
  printf '{"msg": "test validation warning", "severity": "warning"}\n'
else
  exit 1
fi
