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
from nmr_FAIR_DOs.utils import fetch_multiple

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


class AbstractRepository(ABC):
    """
    An abstract class representing a repository.

    Attributes:
        repositoryID (str): An identifier for the repository
    """

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
    async def getAllAvailableResources(self) -> list[dict] | None:
        """
        Returns a list of all resources available in the repository.

        Returns:
            list[dict]: A list of all resources available in the repository
            None: If no resources are available in the repository
        """
        return NotImplemented

    @abstractmethod
    async def getResourcesForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[dict]:
        """
        Returns a list of all resources available in the repository within the specified time frame.

        Args:
            start (datetime): The start of the time frame
            end (datetime): The end of the time frame

        Returns:
            list[dict]: A list of all resources available in the repository within the specified time frame
        """
        return NotImplemented

    @abstractmethod
    async def extractPIDRecordFromResource(
        self,
        resource: dict,
        addEntries: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord | None:
        """
        Extracts a PID record from a resource of the repository.

        Args:
            resource (dict): The resource to extract the PID record from
            addEntries (function): The function to add entries to a PIDRecord. This function expects the following arguments in the following order: (str, list[PIDRecordEntry]) and returns a str. The first argument is the (presumed) PID of the target record, the second argument is a list of entries to add to the target record. It returns the PID of the target record.

        Returns:
            PIDRecord: The PID record extracted from the resource
            None: If the PID record cannot be extracted from the resource
        """
        return NotImplemented

    @abstractmethod
    def getRepositoryFDO(self) -> PIDRecord:
        """
        Define the PID record for the repository.

        Returns:
            PIDRecord: The PID record for the repository
        """
        return NotImplemented

    async def extractAll(
        self,
        urls: list[str],
        addEntries: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> tuple[list[PIDRecord], list[dict]] | list[PIDRecord]:
        """
        Extracts PID records from all resources available in the repository.

        Args:
            urls (list[str]): A list of URLs for all resources available in the repository. (Optional) If not provided, all available URLs will be fetched from the repository.
            addEntries (function): The function to add entries to a PIDRecord. This function expects the following arguments in the following order: (str, list[PIDRecordEntry]) and returns a str. The first argument is the (presumed) PID of the target record, the second argument is a list of entries to add to the target record. It returns the PID of the target record.

        Returns:
            tuple[list[PIDRecord], list[dict[str, str]]]: A tuple containing a list of extracted PID records and a list of errors encountered during extraction
            list[PIDRecord]: A list of extracted PID records
        """
        resources = []

        if urls is None or not isinstance(urls, list) or len(urls) == 0:
            try:
                resources = await self.getAllAvailableResources()
            except Exception as e:
                logger.error(
                    f"Error getting resources from repository {self.repositoryID}: {str(e)}"
                )
                return []
        else:
            resources = await fetch_multiple(urls)

        if resources is None or len(urls) == 0:
            logger.warning(f"No resources available for repository {self.repositoryID}")
            return []

        pid_records: list[PIDRecord] = []
        errors: list[dict] = []

        for resource in resources:
            try:
                pid_record = await self.extractPIDRecordFromResource(
                    resource,
                    addEntries,
                )
                if pid_record is not None:
                    pid_records.append(pid_record)
            except Exception as e:
                logger.error(f"Error extracting PID record from {resource}: {str(e)}")
                errors.append(
                    {
                        "url": resource,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        if errors:
            return pid_records, errors
        return pid_records
