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

from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry


class PIDRecord:
    """ "
    This class represents a PID record with a PID and entries.
    For more information on the PID record format, see the documentation of the Typed PID Maker (https://kit-data-manager.github.io/webpage/typed-pid-maker/openapi.html)
    """

    _pid: str
    _entries: dict[str, list[PIDRecordEntry]]

    def __init__(self, pid: str, entries: dict | list[PIDRecordEntry] = None):
        """
        Creates a PID record

        :param pid:Str The PID of the PID record
        :param entries:dict The entries of the PID record (optional) Entries is a dictionary with the key as the key of the entry and the value as a list of values for the entry. Each value is a dictionary with the key "value" and the value of the entry as the value. The value can also be accessed with the key "@value"

        :raises ValueError: If the PID is None
        """
        if pid is None:
            raise ValueError("PID must not be None")

        self._pid = pid

        self._entries = {}
        if entries is not None and isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, PIDRecordEntry):
                    self.addPIDRecordEntry(entry)
                elif isinstance(entry, dict) and "key" in entry and "value" in entry:
                    self.addEntry(
                        entry["key"],
                        entry["value"],
                        entry["name"] if "name" in entry else None,
                    )

    def addPIDRecordEntry(self, entry: PIDRecordEntry):
        """
        Adds a PID record entry to the PID record

        :param entry:PIDRecordEntry The PID record entry to add

        :raises ValueError: If the key of the entry is None or the value of the entry is None
        """
        if entry.key is None:
            raise ValueError("Key must not be None")
        if entry.value is None:
            raise ValueError("Value must not be None")

        if entry.key not in self._entries:
            self._entries[entry.key] = [entry]
        elif isinstance(self._entries[entry.key], list):
            if entry not in self._entries[entry.key]:
                self._entries[entry.key].append(entry)
        else:
            self._entries[entry.key] = [self._entries[entry.key], entry]

    def addEntry(self, key: str, value: str, name: str = None):
        """
        Adds an entry to the PID record
        If the entry already exists, it is not added again (no duplicates)

        :param key:str The key of the entry
        :param value:str The value of the entry
        :param name:str The name of the entry (optional)

        :raises ValueError: If the key is None or the values are None
        """
        entry = PIDRecordEntry(key, value, name)
        self.addPIDRecordEntry(entry)

    def addListOfEntries(self, entries: list[PIDRecordEntry]):
        """
        Adds multiple PID record entries to the PID record

        :param entries:list[PIDRecordEntry] The PID record entries to add

        :raises ValueError: If the entries are None
        """
        if entries is None:
            raise ValueError("Entries must not be None")

        for entry in entries:
            self.addPIDRecordEntry(entry)

    def addEntries(self, key: str, values: list[str], name: str = None):
        """
        Adds multiple entries to the PID record

        :param key:str The key of the entries
        :param values:list[str] The values of the entries
        :param name: The name of the entries (optional)

        :raises ValueError: If the key is None or the values are None
        """
        if key is None:
            raise ValueError("Key must not be None")

        if values is None:
            raise ValueError("Values must not be None")

        for value in values:
            self.addEntry(key, value, name)

    def getEntries(self) -> dict:
        """
        Returns the entries of the PID record

        :return:dict The entries of the PID record
        """
        return self._entries

    def getPID(self) -> str:
        """
        Returns the PID of the PID record

        :return:str The PID of the PID record
        """
        return self._pid

    def getEntry(self, key: str) -> list[PIDRecordEntry] | PIDRecordEntry | None:
        """
        Returns all entries with the given key

        :param key:str The key of the entries

        :return:list[dict] The entries with the given key
        :return:dict The entry with the given key if only one entry is found
        :return:None If no entry is found

        :raises ValueError: If the key is None
        """
        if key is None:
            raise ValueError("Key must not be None")

        if key in self._entries:
            return self._entries[key]
        else:
            return None

    def deleteEntry(self, key: str, value: str = None):
        """
        Deletes an entry from the PID record

        :param key:str The key of the entry
        :param value: The value of the entry (optional) If the value is None, all entries with the given key are deleted. If the value is not None, only the entry with the given key and value is deleted.

        :raises ValueError: If the key is None
        """
        if key is None:
            raise ValueError("Key must not be None")

        if key in self._entries:
            if value is None:
                del self._entries[key]
            else:
                self._entries[key] = [
                    entry for entry in self._entries[key] if entry["value"] != value
                ]

    def deleteAllEntries(self):
        """
        Deletes all entries from the PID record
        """
        self._entries = {}

    def entryExists(self, key: str, value: str = None) -> bool:
        """
        Checks if an entry exists

        :param key:str The key of the entry
        :param value: The value of the entry (optional) If the value is None, the method checks if an entry with the given key exists. If the value is not None, the method checks if an entry with the given key and value exists.

        :raises ValueError: If the key is None

        :return:bool True if the entry exists, False otherwise
        """
        if key is None:
            raise ValueError("Key must not be None")

        if key in self._entries:
            if value is None:
                return True
            else:
                return any(entry["value"] == value for entry in self._entries[key])
        else:
            return False

    def exportJSON(self) -> dict:
        """
        Exports the PID record as JSON object

        :return:dict The PID record as JSON object
        """
        entries = {}

        for key, value in self._entries.items():
            entries[key] = [entry.exportJSON() for entry in value]

        return {"pid": self._pid, "entries": self._entries}

    def exportSimpleFormatJSON(self) -> dict:
        """
        Exports the PID record as a simple JSON object

        :return:dict The PID record as a simple JSON object
        """
        kv_pairs = []

        for key, value in self._entries.items():
            for entry in value:
                kv_pairs.append({"key": key, "value": entry["value"]})

        return {"pid": self._pid, "record": kv_pairs}

    @staticmethod
    def fromJSON(json: dict) -> "PIDRecord":
        """
        Creates a PID record from a JSON object

        :param json:dict The JSON object to create the PID record from

        :return:PIDRecord The PID record created from the JSON object

        :raises ValueError: If the JSON object is None
        """
        if json is None:
            raise ValueError("JSON must not be None")

        if "pid" not in json:
            raise ValueError("PID must be in JSON object")

        if "entries" not in json:
            return PIDRecord(json["pid"])
        else:
            return PIDRecord(json["pid"], json["entries"])

    def __str__(self):
        return f"PIDRecord(pid={self._pid}, entries={self._entries})"

    def __repr__(self):
        return str(self.exportJSON())
