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


class PIDRecordEntry(dict):
    """
    Represents a PID record entry
    For more information on the PID record format, see the documentation of the Typed PID Maker (https://kit-data-manager.github.io/webpage/typed-pid-maker/openapi.html)

    :param key:str The key of the entry
    :param value:str The value of the entry
    :param name:str The name of the entry (optional)
    """

    key: str
    value: str
    name: str

    def __init__(self, key: str, value: str, name: str = None):
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

        self.key = key
        self.value = value
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
        return json.dumps({"key": self.key, "value": self.value, "name": self.name})

    def __repr__(self):
        return json.dumps({"key": self.key, "value": self.value, "name": self.name})

    def toJSON(self):
        """
        Exports the PID record entry as JSON

        :return:dict The PID record entry as JSON
        """
        if self.name is None:
            return {"key": self.key, "value": self.value}
        else:
            return {"key": self.key, "value": self.value, "name": self.name}

    def __dict__(self):
        return {"key": self.key, "value": self.value, "name": self.name}
