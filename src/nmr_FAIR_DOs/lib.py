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
import os
from datetime import datetime

from dotenv import load_dotenv

from nmr_FAIR_DOs.connectors.elasticsearch import ElasticsearchConnector
from nmr_FAIR_DOs.connectors.tpm_connector import TPMConnector
from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository
from nmr_FAIR_DOs.repositories.chemotion import ChemotionRepository

logger = logging.getLogger(__name__)

load_dotenv()

TPM_URL = os.getenv("TPM_URL")
CHEMOTION_BASE_URL = os.getenv("CHEMOTION_BASE_URL")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX")
ELASTICSEARCH_APIKEY = os.getenv("ELASTICSEARCH_APIKEY")

tpm = TPMConnector(TPM_URL)
chemotion_repo = ChemotionRepository(CHEMOTION_BASE_URL)
elasticsearch = ElasticsearchConnector(
    ELASTICSEARCH_URL, ELASTICSEARCH_APIKEY, ELASTICSEARCH_INDEX
)

pid_records: list[PIDRecord] = []
future_entries: list = []


def getRepository(repo: str) -> AbstractRepository:
    """
    Get the repository object for the given repository name.

    Args:
        repo (str): The name of the repository

    Returns:
        AbstractRepository: The repository object
    """
    if repo == "chemotion":
        return chemotion_repo
    else:
        raise ValueError("Repository not found", repo)


def addEntries(presumed_pid: str, entries: list[PIDRecordEntry]) -> str:
    """
    Add entries to a PID record

    Args:
        presumed_pid (str): The PID of the target PID record
        entries (list[PIDRecordEntry]): The entries to add

    Returns:
        str: The PID of the PID record
    """
    logger.debug(f"Adding entries to PID record with PID {presumed_pid}.", entries)

    # Check if the PID record already exists in the list of PID records
    for record in pid_records:
        if (
            record.getPID() == presumed_pid
        ):  # PID of the record matches the presumed PID
            logger.debug(
                f"Adding entries to existing record with PID {presumed_pid}. Identified by PID.",
                entries,
            )
            record.addListOfEntries(entries)
            return presumed_pid
        elif record.entryExists(
            "21.T11148/b8457812905b83046284", presumed_pid
        ):  # The value of digitalObjectLocation matches the presumed PID
            logger.debug(
                f"Adding entries to existing record with PID {presumed_pid}. Identified by digitalObjectLocation.",
                entries,
            )
            record.addListOfEntries(entries)
            return record.getPID()
    try:
        logger.info(
            "Couldn't find a record to add entries to. Calling addEntries function. Starting to search in elasticsearch."
        )
        pid = elasticsearch.searchForPID(presumed_pid)

        if pid is not None:
            logger.info(
                f"Found PID record in Elasticsearch with PID {pid}. Adding entries to it."
            )
            result = tpm.getPIDRecord(pid)

            if result is not None and isinstance(result, PIDRecord):
                result.addListOfEntries(entries)
                pid_records.append(result)
                tpm.updatePIDRecord(result)
                return result.getPID()
    except Exception as e:
        logger.error("Error adding entries to PID record", presumed_pid, e)
        raise Exception("Error adding entries to PID record", presumed_pid, e)

    logger.info(
        f"Could not find a PID record in Elasticsearch with PID {presumed_pid}. Reminding entry for future use.",
        {"presumed_pid": presumed_pid, "entries": entries},
    )
    future_entries.append({"presumed_pid": presumed_pid, "entries": entries})
    raise Exception(
        "Could not find a PID record in Elasticsearch with PID. Reminding entry for future use.",
        {"presumed_pid": presumed_pid, "entries": entries},
    )


async def create_pidRecords_from_urls(
    repo: AbstractRepository, urls: list[str]
) -> list[PIDRecord]:
    """
    Create PID records for the given URLs.

    Args:
        repo (AbstractRepository): The repository to get the resources from
        urls (list[str]): The URLs to create PID records for

    Returns:
        list[PIDRecord]: A list of PID records created from the URLs

    Raises:
        Exception: If an error occurs during the creation of the PID records
    """
    local_pid_records = []
    errors: list[dict] = []

    logger.info(f"Creating PID records for the following URLs: {urls}")

    # Extract PID records from the resources
    for url in urls:
        logger.info(f"Extracting PID record from {url}")
        try:
            pid_record = await repo.extractPIDRecordFromResource(url, addEntries)
            if pid_record is not None:
                local_pid_records.append(pid_record)
            else:
                logger.error(f"No PID record extracted from {url}")
                errors.append(
                    {
                        "url": url,
                        "error": "No PID record extracted",
                        "timestamp": datetime.now().isoformat(),
                    }
                )
        except Exception as e:
            logger.error(f"Error extracting PID record from {url}: {str(e)}", e)
            errors.append(
                {
                    "url": url,
                    "error": e.__repr__(),
                    "timestamp": datetime.now().isoformat(),
                }
            )

    # Add entries from future entries to PID records
    visited_pids: list = []
    while len(future_entries) > 0:
        entry = future_entries.pop()
        if entry["presumed_pid"] in visited_pids:
            logger.error(
                f"Error adding entries to PID record with PID {entry['presumed_pid']} from future entries.",
                entry,
            )
            errors.append(
                {
                    "url": entry["presumed_pid"],
                    "error": "Error adding entries to PID record with PID from future entries",
                    "timestamp": datetime.now().isoformat(),
                }
            )
        else:
            visited_pids.append(entry["presumed_pid"])
            logger.info(
                f"Adding entries to PID record with PID {entry['presumed_pid']} from future entries.",
                entry,
            )
            addEntries(entry["presumed_pid"], entry["entries"])
            # raise Exception("Error adding entries to PID record with PID from future entries")

    # Create PID records in TPM and add them to Elasticsearch
    real_pid_records = []
    try:
        real_pid_records = tpm.createMultipleFAIRDOs(
            local_pid_records
        )  # create PID records in TPM
    except Exception as e:
        logger.error("Error creating PID records in TPM", local_pid_records, e)
        errors.append({"error": e.__repr__(), "timestamp": datetime.now().isoformat()})

    try:
        elasticsearch.addPIDRecords(
            real_pid_records
        )  # add PID records to Elasticsearch
    except Exception as e:
        logger.error(f"Error adding PID records to Elasticsearch: {str(e)}")
        errors.append({"error": e.__repr__(), "timestamp": datetime.now().isoformat()})

    logger.debug("PID records created:", real_pid_records)
    pid_records.extend(real_pid_records)  # add PID records to the list of PID records

    # write errors to file
    with open("errors_" + repo.repositoryID.replace("/", "_") + ".json", "w") as f:
        json.dump(errors, f)
        logger.info("Errors written to file errors.json")

    with open("pid_records_" + repo.repositoryID.replace("/", "_") + ".json", "w") as f:
        json.dump([record.exportJSON() for record in pid_records], f)
        logger.info("PID records written to file pid_records.json")

    return real_pid_records


async def create_pidRecords_from_scratch(
    repo: AbstractRepository, start: datetime = None, end: datetime = None
) -> list[PIDRecord]:
    """
    Create PID records from scratch for the given time frame.

    Args:
        repo (AbstractRepository): The repository to get the resources from
        start (datetime): The start of the time frame
        end (datetime): The end of the time frame

    Returns:
        list[PIDRecord]: A list of PID records created from scratch

    Raises:
        Exception: If an error occurs during the creation of the PID records
    """
    urls = []

    # Get the URLs for the given time frame or all available URLs if no time frame is given
    if (
        start is not None
        and end is not None
        and isinstance(start, datetime)
        and isinstance(end, datetime)
    ):
        urls = await repo.listURLsForTimeFrame(start, end)
    else:
        urls = await repo.listAvailableURLs()

    logger.info("Creating PID records from scratch for the following URLs:", urls)

    return await create_pidRecords_from_urls(repo, urls)


async def recreate_pidRecords_with_errors(repo: AbstractRepository) -> list[PIDRecord]:
    """
    Recreate PID records for the URLs that caused errors during the last run.

    Returns:
        list[PIDRecord]: A list of PID records created from scratch
    """
    try:
        with open("errors_" + repo.repositoryID + ".json", "r") as f:
            errors = json.load(f)  # read errors from the file
            urls = [
                error["url"] for error in errors if "url" in error
            ]  # get URLs from errors
            return await create_pidRecords_from_urls(
                repo, urls
            )  # create PID records for the URLs
    except Exception as e:
        logger.error(f"Error reading errors.json: {str(e)}")
        return []
