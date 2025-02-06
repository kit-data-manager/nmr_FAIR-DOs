#  SPDX-FileCopyrightText: 2025 Karlsruhe Institute of Technology <maximilian.inckmann@kit.edu>
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright (c) 2025. Karlsruhe Institute of Technology
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

import requests

typeMappings: dict[str, str] = {"URL": "URL"}

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


async def extractDataTypeNameFromPID(pid):
    # Check if the data type name is already known
    if pid in typeMappings:
        # Return the known data type name
        return typeMappings[pid]
    else:
        url = requests.get("https://hdl.handle.net/" + pid).url
        url = url.replace("#", "")
        # url = "https://dtr-test.pidconsortium.net/objects/" + pid
        logger.debug("Requesting data type name from Data Type Registry: " + url)
        # Get the data type name from the Data Type Registry
        response = requests.get(url)
        response_json = response.json()

        name = response_json["name"] if "name" in response_json else pid
        # Store the data type name in the typeMappings dictionary
        typeMappings[pid] = name
        # Return the data type name
        return name
