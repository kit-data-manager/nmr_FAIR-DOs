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
from datetime import datetime
from enum import Enum
from typing import Callable

from nmr_FAIR_DOs.connectors.elasticsearch import ElasticsearchConnector
from nmr_FAIR_DOs.connectors.terminology import Terminology
from nmr_FAIR_DOs.connectors.tpm_connector import TPMConnector
from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry
from nmr_FAIR_DOs.env import (
    TPM_URL,
    CHEMOTION_BASE_URL,
    NMRXIV_BASE_URL,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_APIKEY,
    ELASTICSEARCH_INDEX,
    TERMINOLOGY_URL,
)
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository
from nmr_FAIR_DOs.repositories.chemotion import ChemotionRepository
from nmr_FAIR_DOs.repositories.nmrxiv import NMRXivRepository
from nmr_FAIR_DOs.utils import decodeFromBase64

logger = logging.getLogger(__name__)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

tpm = TPMConnector(TPM_URL)
chemotion_repo = ChemotionRepository(CHEMOTION_BASE_URL)
terminology = Terminology(TERMINOLOGY_URL)
nmrxiv_repo = NMRXivRepository(NMRXIV_BASE_URL, terminology, False)
elasticsearch = ElasticsearchConnector(
    ELASTICSEARCH_URL, ELASTICSEARCH_APIKEY, ELASTICSEARCH_INDEX
)

pid_records: list[PIDRecord] = []
records_to_create: list[PIDRecord] = []
future_entries: list = []
errors: list[dict] = []


class _REPOSITORIES(Enum):
    CHEMOTION = chemotion_repo
    NMRXIV = nmrxiv_repo


def getRepository(repo: str) -> AbstractRepository:
    """
    Get the repository object for the given repository name.

    Args:
        repo (str): The name of the repository

    Returns:
        AbstractRepository: The repository object
    """
    for repository in _REPOSITORIES:
        if repository.name == repo.upper():
            logger.info(f"Found repository {repository.name}")
            return repository.value
    raise ValueError("Repository not found", repo)


def getRepositories(repos: str | list[str] | None) -> list[AbstractRepository]:
    """
    Get the repository objects for the given repository names.

    Args:
        repos (str| list[str] | None): The name of the repositories or None to get all repositories

    Returns:
        list[AbstractRepository]: The repository objects

    Raises:
        ValueError: If the input is invalid
    """
    if repos is None:
        return [repo.value for repo in _REPOSITORIES]
    elif isinstance(repos, str):
        return [getRepository(repos)]
    elif isinstance(repos, list):
        if len(repos) == 1 and repos[0] is None:
            logger.info("Getting all repositories")
            return [repo.value for repo in _REPOSITORIES]
        return [getRepository(repo) for repo in repos]
    raise ValueError("Invalid input", repos)


def addEntries(
    presumed_pid: str,
    entries: list[PIDRecordEntry],
    onSuccess: Callable[[str], None] | None = None,
    allowRetry: bool = True,
) -> str:
    """
    Add entries to a PID record

    Args:
        presumed_pid (str): The PID of the target PID record
        entries (list[PIDRecordEntry]): The entries to add
        onSuccess (function): The function to call after the entries have been added to the PID record (optional). This function gets the PID of the PID record as an argument.

    Returns:
        str: The PID of the PID record
    """
    logger.debug(f"Adding entries to PID record with PID {presumed_pid}.", entries)

    cleartext_presumed_pid = decodeFromBase64(presumed_pid)

    # Check if the PID record already exists in the list of PID records to be created
    for record in records_to_create:
        if (
            record.getPID() == presumed_pid
        ):  # PID of the record matches the presumed PID
            logger.debug(
                f"Adding entries to record in creation with PID {presumed_pid}. Identified by PID.",
                entries,
            )
            record.addListOfEntries(entries)
            if onSuccess is not None and callable(onSuccess):
                onSuccess(record.getPID())
            return record.getPID()
        elif record.entryExists(
            "21.T11148/b8457812905b83046284", cleartext_presumed_pid
        ):  # The value of digitalObjectLocation matches the presumed PID
            logger.debug(
                f"Adding entries to record in creation with PID {presumed_pid}. Identified by digitalObjectLocation.",
                entries,
            )
            record.addListOfEntries(entries)
            if onSuccess is not None and callable(onSuccess):
                onSuccess(record.getPID())
            return record.getPID()

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
            tpm.updatePIDRecord(record)
            if onSuccess is not None and callable(onSuccess):
                onSuccess(record.getPID())
            return record.getPID()
        elif record.entryExists(
            "21.T11148/b8457812905b83046284", cleartext_presumed_pid
        ):  # The value of digitalObjectLocation matches the presumed PID
            logger.debug(
                f"Adding entries to existing record with PID {presumed_pid}. Identified by digitalObjectLocation.",
                entries,
            )
            record.addListOfEntries(entries)
            tpm.updatePIDRecord(record)  # update PID record in the Typed PID-Maker
            if onSuccess is not None and callable(onSuccess):
                onSuccess(record.getPID())
            return record.getPID()

    # Check if the PID record exists in Elasticsearch and get it from the Typed PID-Maker
    try:
        logger.info(
            "Couldn't find a record to add entries to. Calling addEntries function. Starting to search in elasticsearch."
        )
        pid = elasticsearch.searchForPID(cleartext_presumed_pid)

        if pid is not None:
            logger.info(
                f"Found PID record in Elasticsearch with PID {pid}. Adding entries to it."
            )
            record = tpm.getPIDRecord(pid)

            if record is not None and isinstance(record, PIDRecord):
                record.addListOfEntries(entries)
                pid_records.append(record)
                tpm.updatePIDRecord(record)
                if onSuccess is not None and callable(onSuccess):
                    onSuccess(record.getPID())
                return record.getPID()
    except Exception as e:
        if allowRetry:
            # logger.error("Error adding entries to PID record ", presumed_pid, e)
            # raise Exception("Error adding entries to PID record", presumed_pid, e)
            future_entry = {"presumed_pid": presumed_pid, "entries": entries}
            logger.info(
                f"Could not find a PID record locally or in Elasticsearch with PID {presumed_pid} aka {cleartext_presumed_pid}. Reminding entry for future use. {str(e)}",
                future_entry,
                pid_records,
                e,
            )
            if presumed_pid is not None or entries is not None:
                future_entries.append(future_entry)

            # raise Exception(
            #     "Could not find a PID record in Elasticsearch with PID. Reminding entry for future use.",
            #     future_entry,
            # )
        else:
            logger.error(
                f"Retry disabled. Error adding entries to PID record {presumed_pid}. {str(e)}",
                entries,
            )
            raise Exception(
                "Error adding entries to PID record. Retry disabled.",
                presumed_pid,
                entries,
                e,
            )

    return "None"


async def create_pidRecords_from_resources(
    repo: AbstractRepository, resources: list[dict]
):
    """
    Create PID records for the given URLs.

    Args:
        repo (AbstractRepository): The repository to get the resources from
        resources (list[str]): The pre-extracted resources to create PID records from

    Returns:
        list[PIDRecord]: A list of PID records created from the URLs

    Raises:
        Exception: If an error occurs during the creation of the PID records
    """
    logger.info(f"Creating PID records for the {len(resources)} resources")

    # get repository FDO
    repo_FDO, isNew = await _getRepoFDO(repo)

    # Extract PID records from the resources
    for resource in resources:
        logger.debug(f"Extracting PID record from {str(resource)[:100]}")
        try:
            pid_record = await repo.extractPIDRecordFromResource(resource, addEntries)
            if pid_record is not None and isinstance(pid_record, PIDRecord):
                pid_record.addEntry(
                    "21.T11148/a753134738da82809fc1",
                    repo_FDO.getPID(),
                    "hadPrimarySource",
                )
                repo_FDO.addEntry(
                    "21.T11148/4fe7cde52629b61e3b82",
                    pid_record.getPID(),
                    "isMetadataFor",
                )
                records_to_create.append(pid_record)
            else:
                logger.error(f"No PID record extracted from {resource}")
                errors.append(
                    {
                        "url": resource,
                        "error": "No PID record extracted",
                        "timestamp": datetime.now().isoformat(),
                    }
                )
        except Exception as e:
            logger.error(f"Error extracting PID record from {resource}: {str(e)}", e)
            errors.append(
                {
                    # "url": resource,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )

    logger.info("Dealing with future entries", future_entries)
    # Add entries from future entries to PID records
    while len(future_entries) > 0:  # while there are future entries
        entry = future_entries.pop()  # get the first entry
        try:  # try to add the entries to the PID record
            pid = entry["presumed_pid"]
            entries = entry["entries"]

            logger.info(
                f"Adding entries to PID record with PID {pid} from future entries (second attempt).",
                entries,
            )

            addEntries(pid, entries, None, False)
        except Exception as e:
            logger.error(
                f"Error adding entries to PID record with PID {entry['presumed_pid']} from future entries. This was the second and last attempt.",
                entry,
                e,
            )
            errors.append(
                {
                    "url": entry["presumed_pid"],
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )

    if isNew:
        logger.info(f"Creating repository FDO with preliminary PID {repo_FDO.getPID()}")
        records_to_create.append(repo_FDO)
    else:
        logger.info(f"Updating repository FDO with actual PID {repo_FDO.getPID()}")
        tpm.updatePIDRecord(repo_FDO)
        await elasticsearch.addPIDRecord(repo_FDO)

    # write errors to file
    with open("errors_" + repo.repositoryID.replace("/", "_") + ".json", "w") as f:
        json.dump(errors, f)
        logger.info("Errors written to file errors.json")

    # write PID records to file
    with open(
        "records_to_create" + repo.repositoryID.replace("/", "_") + ".json", "w"
    ) as f:
        json.dump([record.toJSON() for record in records_to_create], f)
        logger.info("PID records written to file records_to_create.json")


async def create_pidRecords_from_scratch(
    repos: list[AbstractRepository],
    start: datetime = None,
    end: datetime = None,
    dryrun: bool = False,
) -> list[PIDRecord]:
    """
    Create PID records from scratch for the given time frame.

    Args:
        repos (list[AbstractRepository]): The repositories to create PID records for
        start (datetime): The start of the time frame
        end (datetime): The end of the time frame
        dryrun (bool): If true, the PID records will not be created in TPM or Elasticsearch

    Returns:
        list[PIDRecord]: A list of PID records created from scratch

    Raises:
        Exception: If an error occurs during the creation of the PID records
    """
    start_time = datetime.now()
    for repo in repos:
        resources = []

        # Get the URLs for the given time frame or all available URLs if no time frame is given
        if (
            start is not None
            and end is not None
            and isinstance(start, datetime)
            and isinstance(end, datetime)
        ):
            resources = await repo.getResourcesForTimeFrame(start, end)
        else:
            with open(
                "last_run_" + repo.repositoryID.replace("/", "_") + ".json", "w"
            ) as f:
                f.write(datetime.now().isoformat())
            resources = await repo.getAllAvailableResources()

        logger.info(f"Creating PID records from scratch for {len(resources)} resources")

        await create_pidRecords_from_resources(repo, resources)

    real_pid_records = await _createRecordsToCreate(dryrun)

    # write PID records to file
    with open("pid_records_all.json", "w") as f:
        json.dump([record.toJSON() for record in pid_records], f)
        logger.info("PID records written to file pid_records.json")

    logger.info(
        f"Finished creating PID records for {len(real_pid_records)} URLs in {datetime.now() - start_time}"
    )
    return real_pid_records


async def add_all_existing_pidRecords_to_elasticsearch(fromFile: str = None) -> None:
    """
    Add all existing PID records to Elasticsearch.

    Args:
        fromFile (bool): If true, the PID records will be read from a file instead

    Raises:
        Exception: If an error occurs during the addition of the PID records to Elasticsearch
    """
    try:
        records: list[PIDRecord]
        if fromFile is not None:
            with open(fromFile, "r") as f:
                records = [PIDRecord.fromJSON(record) for record in json.load(f)]
                logger.info(f"found {len(records)} PID records in file {fromFile}")
        else:
            records = await tpm.getAllPIDRecords()
            logger.info(f"found {len(records)} PID records in TPM")

        logger.info("Adding all existing PID records to Elasticsearch")
        await elasticsearch.addPIDRecords(records)

        with open("pid_records_all.json", "w") as f:
            json.dump([record.toJSON() for record in records], f)
            logger.info("PID records written to file pid_records_all.json")
    except Exception as e:
        logger.error("Error adding PID records to Elasticsearch")
        raise Exception("Error adding PID records to Elasticsearch", e)


# async def recreate_pidRecords_with_errors(repo: AbstractRepository) -> list[PIDRecord]:
#     """
#     Recreate PID records for the URLs that caused errors during the last run.
#
#     Returns:
#         list[PIDRecord]: A list of PID records created from scratch
#     """
#     try:
#         with open("errors_" + repo.repositoryID + ".json", "r") as f:
#             errors = json.load(f)  # read errors from the file
#             # urls = [
#             #     for
#             #     error["url"] for error in errors if "url" in error
#             # ]  # get URLs from errors
#
#             urls = []
#             for error in errors:
#                 if "url" in error:
#                     if error["url"] in urls:
#                         continue
#                     elif pid_records is not None and any(
#                         pid_record.getPID() == error["url"]
#                         for pid_record in pid_records
#                     ):
#                         continue
#                     elif pid := elasticsearch.searchForPID(error["url"]) is not None:
#                         record = tpm.getPIDRecord(pid)
#                         if record is not None:
#                             pid_records.append(record)
#                             continue
#                         else:
#                             urls.append(error["url"])
#                     else:
#                         urls.append(error["url"])
#             if len(urls) > 0:
#                 logger.info(
#                     f"Recreating PID records for the following URLs that caused errors during the last run: {urls}"
#                 )
#                 record = await create_pidRecords_from_urls(repo, urls)
#                 pid_records.extend(record)
#
#             return pid_records
#     except Exception as e:
#         logger.error(f"Error reading errors.json: {str(e)}")
#         return []


def _deduplicateListOfPIDRecords(input: list[PIDRecord]) -> list[PIDRecord]:
    """
    Deduplicate a list of PID records

    Args:
        input (list[PIDRecord]): The list of PID records to deduplicate

    Returns:
        list[PIDRecord]: The deduplicated list of PID records
    """
    deduplicated_pid_records: dict[str, PIDRecord] = {}
    for record in input:
        if record.getPID() in deduplicated_pid_records:
            logger.debug(f"Found duplicate PID {record.getPID()}", record)
            deduplicated_pid_records[record.getPID()].merge(record)
        else:
            deduplicated_pid_records[record.getPID()] = record
    return list(deduplicated_pid_records.values())


async def _createRecordsToCreate(dryrun: bool) -> list[PIDRecord]:
    logger.info("Fixing duplicate PIDs")  # Fix duplicate PIDs
    deduplicated_records = _deduplicateListOfPIDRecords(records_to_create)

    with open("deduplicated_records_to_create.json", "w") as f:
        json.dump([record.toJSON() for record in deduplicated_records], f)
        logger.info("PID records written to file deduplicated_records_to_create.json")
    if not dryrun:
        # Create PID records in TPM
        real_pid_records = []
        try:
            logger.info("Creating PID records in TPM")
            real_pid_records = tpm.createMultipleFAIRDOs(
                deduplicated_records
            )  # create PID records in TPM
        except Exception as e:
            logger.error("Error creating PID records in TPM")
            errors.append(
                {"error": e.__repr__(), "timestamp": datetime.now().isoformat()}
            )
        logger.debug("PID records created")
        pid_records.extend(
            real_pid_records
        )  # add PID records to the list of PID records

        # Add PID records to Elasticsearch
        try:
            logger.info("Adding PID records to Elasticsearch")
            await elasticsearch.addPIDRecords(
                real_pid_records
            )  # add PID records to Elasticsearch
        except Exception as e:
            logger.error(f"Error adding PID records to Elasticsearch: {str(e)}")
            errors.append(
                {"error": e.__repr__(), "timestamp": datetime.now().isoformat()}
            )
        return real_pid_records
    else:
        logger.warning("Dryrun: Not creating PID records in TPM or Elasticsearch")
        pid_records.extend(
            deduplicated_records
        )  # add PID records to the list of PID records
        return deduplicated_records


async def _getRepoFDO(repo: AbstractRepository):
    """
    Get the repository FDO for the given repository.

    Args:
        repo (AbstractRepository): The repository to get the repository FDO for

    Returns:
        PIDRecord: The repository FDO
        bool: Whether the repository FDO is newly created (True) or has to be updated (False)
    """
    new_repo_FDO = repo.getRepositoryFDO()
    try:
        existing_repo_FDO_pid = elasticsearch.searchForPID(new_repo_FDO.getPID())
        actual_repoFDO_pid = None
        if existing_repo_FDO_pid is not None:
            existing_repo_FDO = tpm.getPIDRecord(existing_repo_FDO_pid)
            if existing_repo_FDO is not None:
                actual_repoFDO_pid = existing_repo_FDO.getPID()
                logger.info(
                    f"Found existing repository FDO with PID {actual_repoFDO_pid}"
                )
                existing_repo_FDO.merge(new_repo_FDO)
                return existing_repo_FDO, False
                # tpm.updatePIDRecord(existing_repo_FDO) # TODO: update PID record in the Typed PID-Maker
                # await elasticsearch.addPIDRecord(existing_repo_FDO)
    except Exception as e:
        logger.info(f"Error getting existing repository FDO: {str(e)}")

    logger.info(f"Creating repository FDO with PID {new_repo_FDO.getPID()}")
    return new_repo_FDO, True


if __name__ == "__main__":
    with open("pid_records_all.json", "r") as f:
        pid_records = [PIDRecord.fromJSON(record) for record in json.load(f)]
        logger.info(f"found {len(pid_records)} PID records in file")

        most_informative_FDO = pid_records[0]
        biggest_FDO = pid_records[0]
        biggest_FDO_attributeValue = 0
        for record in pid_records:
            if len(record.getEntries()) > len(most_informative_FDO.getEntries()):
                logger.info(f"New most informative FAIR-DO: {record.getPID()}")
                most_informative_FDO = record

            for entry in record.getEntries().values():
                if isinstance(entry, list) and len(entry) > biggest_FDO_attributeValue:
                    logger.info(f"New biggest FAIR-DO: {record.getPID()}")
                    biggest_FDO = record
                    biggest_FDO_attributeValue = len(entry)

            # if len(record.getEntries().values().) > biggest_FDO_attributeValue:
            #     logger.info(f"New biggest FAIR-DO: {record.getPID()}")
            #     biggest_FDO = record
            #     biggest_FDO_attributeValue = len(record.getEntries().values())

            # # Check all attributes of the record. If the amount of values is bigger than the current biggest_FDO_attributeValue, update the biggest_FDO
            # for entry in record.getEntries().:
            #     if entry.value is not None and isinstance(entry.value, list):
            #         if len(entry.value) > biggest_FDO_attributeValue:
            #             logger.info(f"New biggest FAIR-DO: {record.getPID()}")
            #             biggest_FDO = record
            #             biggest_FDO_attributeValue = max(
            #                 len(entry.value) for entry in record.getEntries()
            #             )

        logger.info(f"Most informative FAIR-DO: {most_informative_FDO.getPID()}")
        with open("most_informative_FDO.json", "w") as f:
            json.dump(most_informative_FDO.toJSON(), f)
            logger.info(
                "Most informative FAIR-DO written to file most_informative_FDO.json"
            )

        logger.info(f"Biggest FAIR-DO: {biggest_FDO.getPID()}")
        with open("biggest_FDO.json", "w") as f:
            json.dump(biggest_FDO.toJSON(), f)
            logger.info("Biggest FAIR-DO written to file biggest_FDO.json")
