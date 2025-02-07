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

from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class PIDRecord:
    """ "
    This class represents a PID record with a PID and entries.
    For more information on the PID record format, see the documentation of the Typed PID Maker (https://kit-data-manager.github.io/webpage/typed-pid-maker/openapi.html)

    Attributes:
        _pid (str): The PID of the PID record
        _entries (dict[str, list[PIDRecordEntry]]): The entries of the PID record. The entries are stored in a dictionary with the key as the key of the entry and the value as a list of values for the entry. Each value is a dictionary with the key "value" and the value of the entry as the value. The value can also be accessed with the key "@value"
    """

    _pid: str
    _entries: dict[str, list[PIDRecordEntry]]

    def __init__(self, pid: str, entries: list[PIDRecordEntry] = None):
        """
        Creates a PID record

        Args:
            pid (str): The PID of the PID record
            entries (list[PIDRecordEntry]): The entries of the PID record (optional) Entries is a dictionary with the key as the key of the entry and the value as a list of values for the entry. Each value is a dictionary with the key "value" and the value of the entry as the value. The value can also be accessed with the key "@value"

        Raises:
            ValueError: If the PID is None
        """
        if pid is None:
            raise ValueError("PID must not be None")

        self._pid = pid

        self._entries = {}
        if entries is not None and isinstance(
            entries, list
        ):  # Check if entries is not None and a list
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

        Args:
            entry (PIDRecordEntry): The PID record entry to add

        Raises:
            ValueError: If the key of the entry is None or the value of the entry is None
        """

        if entry.key is None:  # Check if the key is None
            raise ValueError("Key must not be None")
        if entry.value is None:  # Check if the value is None
            raise ValueError("Value must not be None")

        if (
            entry.key not in self._entries
        ):  # Check if the key is not already in the PID record
            logger.debug(f"Adding entry {entry} to PID record")
            self._entries[entry.key] = [entry]  # Add the entry to the PID record
        elif isinstance(
            self._entries[entry.key], list
        ):  # Check if the entry is already a list
            if not any(
                e.value == entry.value
                for e in self._entries[
                    entry.key
                ]  # Check if the entry value is already in the list
            ):
                logger.debug(
                    f"Adding entry {entry} to PID record. Entry with key {entry.key} already exists. Adding to list"
                )
                self._entries[entry.key].append(
                    entry
                )  # Add the entry to the list iff the value is not already in the list
            logger.debug(
                f"Entry with key {entry.key} and value {entry.value} already exists. Skipping"
            )
        else:  # If the entry is not a list
            logger.debug(
                f"Adding entry {entry} to PID record. Entry with key {entry.key} already exists. Converting to list"
            )
            self._entries[entry.key] = [
                self._entries[entry.key],
                entry,
            ]  # Convert the entry to a list

    def addEntry(self, key: str, value: str | dict, name: str = None):
        """
        Adds an entry to the PID record
        If the entry already exists, it is not added again (no duplicates)

        Args:
            key (str): The key of the entry
            value (str|dict): The value of the entry
            name (str): The name of the entry (optional)

        Raises:
            ValueError: If the key is None or the values are None
        """

        entry = PIDRecordEntry(key, value, name)  # Create a PIDRecordEntry object
        self.addPIDRecordEntry(entry)  # Add the PIDRecordEntry object to the PID record

    def addListOfEntries(self, entries: list[PIDRecordEntry]):
        """
        Adds multiple PID record entries to the PID record

        Args:
            entries (list[PIDRecordEntry]): The PID record entries to add

        Raises:
            ValueError: If the entries are None
        """

        if entries is None:
            raise ValueError("Entries must not be None")

        for entry in entries:  # Add each entry to the PID record
            self.addPIDRecordEntry(entry)

    def addEntries(self, key: str, values: list[str], name: str = None):
        """
        Adds multiple entries to the PID record

        Args:
            key (str): The key of the entries
            values (list[str]): The values of the entries. All values are added to the PID record with the same key.
            name (str): The name of the entries (optional)

        Raises:
            ValueError: If the key is None or the values are None
        """
        if key is None:  # Check if the key is None
            raise ValueError("Key must not be None")

        if values is None:  # Check if the values are None
            raise ValueError("Values must not be None")

        for value in values:  # Add each value to the PID record
            self.addEntry(key, value, name)

    def updateEntry(self, key: str, value: str | dict, name: str = None):
        """
        Updates an entry in the PID record
        If the entry does not exist, it is added

        Args:
            key (str): The key of the entry
            value (str|dict): The value of the entry. If the value is a dictionary, it is converted to a string internally.
            name (str): The name of the entry (optional)

        Raises:
            ValueError: If the key is None
        """
        entry = PIDRecordEntry(key, value, name)  # Create a PIDRecordEntry object
        self.deleteEntry(key)  # Delete the entry with the given key
        self.addPIDRecordEntry(entry)  # Add the PIDRecordEntry object to the PID record

    def getEntries(self) -> dict:
        """
        Returns the entries of the PID record

        Returns:
            dict: The entries of the PID record
        """
        return self._entries

    def getPID(self) -> str:
        """
        Returns the PID of the PID record

        Returns:
            str: The PID of the PID record
        """
        return self._pid

    def getEntry(self, key: str) -> list[PIDRecordEntry] | PIDRecordEntry | None:
        """
        Returns all entries with the given key

        Args:
            key (str): The key of the entries

        Returns:
            list[PIDRecordEntry]: The entries with the given key
            PIDRecordEntry: The entry with the given key if only one entry is found
            None: If no entry is found

        Raises:
            ValueError: If the key is None
        """
        if key is None:  # Check if the key is None
            raise ValueError("Key must not be None")

        if key in self._entries:  # Check if the key is in the PID record
            return self._entries[key]
        else:  # If the key is not in the PID record
            return None

    def deleteEntry(self, key: str, value: str | dict = None):
        """
        Deletes an entry from the PID record

        Args:
            key (str): The key of the entry
            value (str|dict): The value of the entry (optional) If the value is None, all entries with the given key are deleted. If the value is not None, only the entry with the given key and value is deleted.

        Raises:
            ValueError: If the key is None
        """
        if key is None:  # Check if the key is None
            raise ValueError("Key must not be None")

        if key in self._entries:
            if value is None:  # Delete all entries with the given key
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

    def entryExists(self, key: str, value: str | dict = None) -> bool:
        """
        Checks if an entry exists

        Args:
            key (str): The key of the entry
            value (str|dict): The value of the entry (optional) If the value is None, the method checks if an entry with the given key exists. If the value is not None, the method checks if an entry with the given key and value exists.

        Returns:
            bool: True if the entry exists, False otherwise

        Raises:
            ValueError: If the key is None
        """
        if key is None:  # Check if the key is None
            raise ValueError("Key must not be None")

        if key in self._entries:  # Check if the key is in the PID record
            if value is None:  # Check if the value argument is not specified (None)
                return True
            else:  # If the value argument is specified, check if the value is in the list of entries
                return any(entry["value"] == value for entry in self._entries[key])
        else:  # If the key is not in the PID record
            return False

    def toJSON(self) -> dict:
        """
        Exports the PID record as JSON object

        Returns:
            dict: The PID record as JSON object
        """
        entries = {}

        for key, value in self._entries.items():  # Iterate over all entries
            entries[key] = [
                entry.toJSON() for entry in value
            ]  # Convert the entries to JSON

        return {"pid": self._pid, "entries": entries}

    def exportSimpleFormatJSON(self) -> dict:
        """
        Exports the PID record as a simple JSON object

        Returns:
            dict: The PID record as a simple JSON object
        """
        kv_pairs = []

        for key, value in self._entries.items():  # Iterate over all entries
            for entry in value:  # Iterate over all values of the entry
                kv_pairs.append(
                    {"key": key, "value": entry["value"]}
                )  # Add the key and value to the list

        return {"pid": self._pid, "record": kv_pairs}

    @staticmethod
    def fromJSON(input_json: dict) -> "PIDRecord":
        """
        Creates a PID record from a JSON object

        Args:
            input_json (dict): The JSON object to create the PID record from

        Returns:
            PIDRecord: The PID record created from the JSON object

        Raises:
            ValueError: If the JSON object is None or invalid
        """
        logger.debug("Trying to extract PID record from JSON", input_json)

        if input_json is None:  # Check if the JSON object is None
            raise ValueError("JSON must not be None")

        if "pid" not in input_json:  # Check if the JSON object contains a PID
            raise ValueError("PID must be in JSON object")

        if "entries" not in input_json:  # Check if the JSON object contains entries
            return PIDRecord(input_json["pid"])
        else:
            entries = []

            for key, value in input_json[
                "entries"
            ].items():  # Iterate over all entries in the JSON object
                for entry in value:  # Iterate over all values of the entry. Each value consists of a dict with the keys "key", "value" and "name". Name is the only optional key.
                    if "value" not in entry or "key" not in entry:
                        # Skip this entry if it does not contain a key or value
                        logger.warning(
                            f"Skipping entry {entry} because it does not contain a key or value"
                        )
                        continue
                    elif "name" in entry:
                        # If the entry contains a name, add it to the PIDRecordEntry
                        entries.append(
                            PIDRecordEntry(key, entry["value"], entry["name"])
                        )
                    else:
                        # If the entry does not contain a name, add it without a name
                        entries.append(PIDRecordEntry(key, entry["value"]))

            logger.debug(
                f"Extracted PID record from JSON: {PIDRecord(input_json["pid"], entries)}"
            )
            return PIDRecord(input_json["pid"], entries)

    def merge(self, other: "PIDRecord") -> "PIDRecord":
        """
        Merges the PID record with another PID record

        Args:
            other (PIDRecord): The PID record to merge with

        Returns:
            PIDRecord: The merged PID record

        Raises:
            ValueError: If the other PID record is None
        """
        if other is None:  # Check if the other PID record is None
            raise ValueError("Other PID record must not be None")

        if (
            self._pid != other.getPID()
        ):  # Check if the PID of both PID records is the same
            raise ValueError("PID of both PID records must be the same")

        for (
            key,
            value,
        ) in (
            other.getEntries().items()
        ):  # Iterate over all entries in the other PID record
            for entry in value:  # Iterate over all values of the entry
                if not self.entryExists(
                    key, entry.value
                ):  # Check if the entry does not exist in this PID record
                    self.addPIDRecordEntry(entry)  # Add the entry to this PID record

        return self  # Return the merged PID record

    def __str__(self):
        return f"PIDRecord(pid={self._pid}, entries={self._entries})"

    def __repr__(self):
        return str(self.toJSON())

    def __dict__(self):
        return self.toJSON()
