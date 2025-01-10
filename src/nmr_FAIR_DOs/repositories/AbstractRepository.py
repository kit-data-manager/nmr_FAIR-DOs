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
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable

from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


class AbstractRepository(ABC):
    @property
    @abstractmethod
    def repositoryID(self) -> str:
        """
        Returns an identifier for the repository.

        Returns:
            str: The id of the repository
        """
        return NotImplemented

    @abstractmethod
    async def listAvailableURLs(self) -> list[str] | None:
        """
        Returns a list of URLs for all resources available in the repository.
        These URLs can be used to extract metadata from the resources.

        Returns:
            list[str]: A list of URLs for all resources available in the repository
            None: If no resources are available in the repository
        """
        return NotImplemented

    @abstractmethod
    async def listURLsForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[str] | None:
        """
        Returns a list of URLs for all resources available in the repository within the specified time frame.
        These URLs can be used to extract metadata from the resources.

        Args:
            start (datetime): The start of the time frame
            end (datetime): The end of the time frame

        Returns:
            list[str]: A list of URLs for all resources available in the repository within the specified time frame
            None: If no resources are available in the repository within the specified time frame
        """
        return NotImplemented

    @abstractmethod
    async def extractPIDRecordFromResource(
        self, url: str, addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> PIDRecord | None:
        """
        Extracts a PID record from the resource available at the specified URL.

        Args:
            url (str): The URL of the resource
            addEntries (function): The function to add entries to a PIDRecord. This function expects the following arguments in the following order: (str, list[PIDRecordEntry]) and returns a str. The first argument is the (presumed) PID of the target record, the second argument is a list of entries to add to the target record. It returns the PID of the target record.

        Returns:
            PIDRecord: The PID record extracted from the resource
            None: If the PID record cannot be extracted from the resource
        """
        return NotImplemented

    async def extractAll(
        self, urls: list[str], addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> tuple[list[PIDRecord], list[dict[str, str]]] | list[PIDRecord]:
        """
        Extracts PID records from all resources available in the repository.

        Args:
            urls (list[str]): A list of URLs for all resources available in the repository. (Optional) If not provided, all available URLs will be fetched from the repository.
            addEntries (function): The function to add entries to a PIDRecord. This function expects the following arguments in the following order: (str, list[PIDRecordEntry]) and returns a str. The first argument is the (presumed) PID of the target record, the second argument is a list of entries to add to the target record. It returns the PID of the target record.

        Returns:
            tuple[list[PIDRecord], list[dict[str, str]]]: A tuple containing a list of extracted PID records and a list of errors encountered during extraction
            list[PIDRecord]: A list of extracted PID records
        """

        if urls is None or not isinstance(urls, list) or len(urls) == 0:
            try:
                urls = await self.listAvailableURLs()
            except Exception as e:
                logger.error(
                    f"Error fetching URLs from repository {self.repositoryID}: {str(e)}"
                )
                return []

        if urls is None or len(urls) == 0:
            logger.warning(f"No URLs available for repository {self.repositoryID}")
            return []

        pid_records = []
        errors = []

        # def localAddEntries(presumed_pid: str, entries: list[PIDRecordEntry]) -> str:
        #     """
        #     Adds entries to an existing record with the specified PID.
        #     This only applies to records that have already been extracted from the resources.
        #
        #     Args:
        #         presumed_pid (str): The presumed PID of the target record
        #         entries (list[PIDRecordEntry]): A list of entries to add to the target record
        #
        #     Returns:
        #         str: The PID of the target record
        #     """
        #     for record in pid_records:
        #         if record.getPID() == presumed_pid: # PID of the record matches the presumed PID
        #             logger.debug(f"Adding entries to existing record with PID {presumed_pid}. Identified by PID.", entries)
        #             record.addListOfEntries(entries)
        #             return presumed_pid
        #         elif record.entryExists("21.T11148/b8457812905b83046284", presumed_pid): # Value of digitalObjectLocation matches the presumed PID
        #             logger.debug(f"Adding entries to existing record with PID {presumed_pid}. Identified by digitalObjectLocation.", entries)
        #             record.addListOfEntries(entries)
        #             return record.getPID()
        #
        #     logger.info("Couldn't find a record to add entries to. Calling addEntries function.")
        #     return addEntries(presumed_pid, entries)

        for url in urls:
            try:
                # pid_record = await self.extractPIDRecordFromResource(url, localAddEntries)
                pid_record = await self.extractPIDRecordFromResource(url, addEntries)
                if pid_record is not None:
                    pid_records.append(pid_record)
            except Exception as e:
                logger.error(f"Error extracting PID record from {url}: {str(e)}")
                errors.append(
                    {
                        "url": url,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        if errors:
            return pid_records, errors
        return pid_records
