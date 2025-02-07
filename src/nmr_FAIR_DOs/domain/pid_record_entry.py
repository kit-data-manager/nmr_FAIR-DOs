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
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class PIDRecordEntry(dict):
    """
    Represents a PID record entry.
    For more information on the PID record format, see the documentation of the Typed PID Maker (https://kit-data-manager.github.io/webpage/typed-pid-maker/openapi.html).
    A PID record entry consists of a key, a value, and optionally a name.

    Attributes:
    key:str The key of the entry
    value:str The value of the entry
    name:str The name of the entry (optional)
    """

    key: str = None
    value: str | dict = None
    name: str = None

    def __init__(self, key: str, value: str | dict, name: str = None):
        """
        Creates a PID record entry

        Args:
            key:str The key of the entry
            value:str The value of the entry
            name:str The name of the entry (optional)

        Raises:
            ValueError: If the key is None or the value is None
        """

        super().__init__()  # initialize the dictionary
        if key is None:  # if key is None, raise an error
            raise ValueError(f"Key must not be None: {self.__repr__()}")

        if value is None:  # if value is None, raise an error
            raise ValueError(f"Value must not be None: {self.__repr__()}")
        elif not isinstance(value, str) and not isinstance(
            value, dict
        ):  # if value is not a string or a dictionary, log a warning
            logger.warning(
                f"Value SHOULD be a string or a dictionary: {key}({name}), {value}"
            )

        try:
            if isinstance(value, str):  # if value is a JSON string, parse it
                self.value = json.loads(value)
            elif isinstance(value, dict):  # if value is a dictionary, use it as is
                self.value = value
            else:  # if the value is neither; parse as string
                self.value = str(value)
        except Exception as e:  # if parsing fails, log a warning
            logger.debug(f"Value is not a JSON string: {value}, {e}")
            self.value = value  # if value is not a JSON string, use it as is

        self.key = key
        self.name = name

    def __getitem__(self, item):
        """
        This method is called to get the value of the given key.
        This enables the use of the dictionary syntax to access the key, value, and name of the PID record entry.
        """
        if item == "key":
            return self.key
        elif item == "value":
            return self.value
        elif item == "name":
            return self.name
        else:
            return None

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        val = (
            json.dumps(self.value) if isinstance(self.value, dict) else self.value
        )  # if the value is a dictionary, convert it to a JSON string
        return json.dumps(
            {"key": self.key, "value": val, "name": self.name}
        )  # return the key, value, and name as a JSON string

    def toJSON(self):
        """
        Exports the PID record entry as JSON

        Returns:
        dict: The PID record entry as JSON
        """
        val = json.dumps(self.value) if isinstance(self.value, dict) else self.value

        if self.name is None:  # if the name is None, return only the key and value
            return {"key": self.key, "value": val}
        else:  # if the name is not None, return the key, value, and name
            return {"key": self.key, "value": val, "name": self.name}

    def __dict__(self):
        val = json.dumps(self.value) if isinstance(self.value, dict) else self.value
        return {"key": self.key, "value": val, "name": self.name}
