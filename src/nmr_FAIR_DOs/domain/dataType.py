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


async def extractDataTypeNameFromPID(pid) -> str:
    """
    Extracts the data type name from a PID.
    This PID is used in the key coloumn of a given PID record and references to a data type (inside a data type registry).
    Inside of the data type registry (DTR), the data type is stored and contains much more information (i.e., description, provenance, regex, etc.).
    For more information on the information available in the data type registry, have a look at [data type registry](https://typeregistry.lab.pidconsortium.net/)

    Args:
        pid (str): The PID to extract the data type name from.

    Returns:
        str: A human-readable name of the data type.
    """
    # Check if the data type name is already known
    if pid in typeMappings:
        # Return the known data type name
        return typeMappings[pid]
    else:  # If the data type name is not known
        # Resolve the PID via the Handle.net resolver. When resolving a data type PID, the user is automatically redirected to the data type registry.
        url = requests.get("https://hdl.handle.net/" + pid).url
        url = url.replace(
            "#", ""
        )  # The URL might contain a hash which signalizes the DTR to render a webpage. This webpage is not useful for the extraction of the data type name.

        # Request the data type from the data type registry
        logger.debug("Requesting data type name from Data Type Registry: " + url)
        response = requests.get(
            url
        )  # Request the data type from the data type registry
        response_json = response.json()

        # Extract the data type name from the response. If the name is not available, return the PID as the name to avoid errors.
        name = response_json["name"] if "name" in response_json else pid
        # Store the data type name in the typeMappings dictionary
        typeMappings[pid] = name
        # Return the data type name
        return name
