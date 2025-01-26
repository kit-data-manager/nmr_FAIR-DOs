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
    Represents a PID record entry
    For more information on the PID record format, see the documentation of the Typed PID Maker (https://kit-data-manager.github.io/webpage/typed-pid-maker/openapi.html)

    :param key:str The key of the entry
    :param value:str The value of the entry
    :param name:str The name of the entry (optional)
    """

    key: str
    value: str | dict
    name: str

    def __init__(self, key: str, value: str | dict, name: str = None):
        """
        Creates a PID record entry

        :param key:str The key of the entry
        :param value:str The value of the entry
        :param name:str The name of the entry (optional)

        :raises ValueError: If the key is None or the value is None
        """

        super().__init__()
        if key is None:
            raise ValueError("Key must not be None")

        if value is None:
            raise ValueError("Value must not be None")
        elif not isinstance(value, str) and not isinstance(value, dict):
            raise ValueError("Value must be a string or a dictionary")

        try:
            if isinstance(value, str):  # if value is a JSON string, parse it
                self.value = json.loads(value)
            else:
                self.value = value
        except Exception as e:
            logger.debug(f"Value is not a JSON string: {value}, {e}")
            self.value = value  # if value is not a JSON string, use it as is

        self.key = key
        # self.value = value
        self.name = name

    def __getitem__(self, item):
        if item == "key":
            return self.key
        elif item == "value":
            return self.value
        elif item == "name":
            return self.name
        else:
            return None

    def __str__(self):
        val = json.dumps(self.value) if isinstance(self.value, dict) else self.value
        return json.dumps({"key": self.key, "value": val, "name": self.name})

    def __repr__(self):
        val = json.dumps(self.value) if isinstance(self.value, dict) else self.value
        return json.dumps({"key": self.key, "value": val, "name": self.name})

    def toJSON(self):
        """
        Exports the PID record entry as JSON

        :return:dict The PID record entry as JSON
        """
        val = json.dumps(self.value) if isinstance(self.value, dict) else self.value

        if self.name is None:
            return {"key": self.key, "value": val}
        else:
            return {"key": self.key, "value": val, "name": self.name}

    def __dict__(self):
        val = json.dumps(self.value) if isinstance(self.value, dict) else self.value
        return {"key": self.key, "value": val, "name": self.name}
