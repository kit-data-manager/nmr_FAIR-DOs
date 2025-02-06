"""
This module contains the main functionality to create PID records from resources and to add them to the Typed PID-Maker and Elasticsearch.
It initializes the repositories and the connectors and provides functions to add entries to PID records and to create PID records from resources or from scratch.
"""
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
nmrxiv_repo = NMRXivRepository(NMRXIV_BASE_URL, terminology)
elasticsearch = ElasticsearchConnector(
    ELASTICSEARCH_URL, ELASTICSEARCH_APIKEY, ELASTICSEARCH_INDEX
)

pid_records: list[PIDRecord] = []
records_to_create: list[PIDRecord] = []
future_entries: list = []
errors: list[dict] = []


class _REPOSITORIES(Enum):
    """
    Enum for all available repositories with their respective repository objects.
    """

    CHEMOTION = chemotion_repo
    NMRXIV = nmrxiv_repo


def getRepository(repo: str) -> AbstractRepository:
    """
    Get the repository object for the given repository name. If the repository is not found, a ValueError is raised.

    Args:
        repo (str): The name of the repository

    Returns:
        AbstractRepository: The repository object

    Raises:
        ValueError: If the repository is not found
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


def addRelationship(
    presumed_pid: str,
    entries: list[PIDRecordEntry],
    onSuccess: Callable[[str], None] | None = None,
    allowRetry: bool = True,
) -> str:
    """
    This method creates a relationship between two FAIR-DOs by adding entries to the PID record with the given PID.
    To accomplish this, the method first checks if the PID record already exists in the list of PID records to be created or in the list of PID records.
    If the PID record is not found, the method searches for the PID record in Elasticsearch and gets it from the Typed PID-Maker.
    If the PID record is still not found, the method raises an exception.
    The method then adds the entries to the PID record and calls the onSuccess function if it is given.
    Usually, this onSuccess function is used to add a relationship from the target PID record to the PID of the PID record that the entries are added to.
    This feature allows the creation of bidirectional relationships between two FAIR-DOs.

    Args:
        presumed_pid (str): The PID of the target PID record.
        entries (list[PIDRecordEntry]): The PIDRecordEntries to add to the PID record. See PIDRecordEntry for more information.
        onSuccess (function): The function to call after the entries have been added to the PID record (optional). This function gets the PID of the PID record as an argument.
        allowRetry (bool): If true, the function will retry to add the entries to the PID record if it fails the first time (optional). Default is True.

    Returns:
        str: The PID of the PID record the entries were added to or "None" if the PID record was not found.

    Raises:
        Exception: If any error occurs during the addition of the entries to the PID record
    """
    logger.debug(f"Adding entries to PID record with PID {presumed_pid}.", entries)

    cleartext_presumed_pid = decodeFromBase64(
        presumed_pid
    )  # decode the presumed PID from base64

    # Check if the PID record already exists in the list of PID records to be created
    for record in records_to_create:
        if (
            record.getPID() == presumed_pid
        ):  # PID of the record matches the presumed PID
            logger.debug(
                f"Adding entries to record in creation with PID {presumed_pid}. Identified by PID.",
                entries,
            )
            record.addListOfEntries(entries)  # add entries to the record
            if onSuccess is not None and callable(
                onSuccess
            ):  # call onSuccess function if it is given and callable
                onSuccess(record.getPID())
            return (
                record.getPID()
            )  # return the PID of the record the entries were added to. SUCCESS
        elif record.entryExists(
            "21.T11148/b8457812905b83046284", cleartext_presumed_pid
        ):  # The value of digitalObjectLocation matches the presumed PID
            logger.debug(
                f"Adding entries to record in creation with PID {presumed_pid}. Identified by digitalObjectLocation.",
                entries,
            )
            record.addListOfEntries(entries)  # add entries to the record
            if onSuccess is not None and callable(
                onSuccess
            ):  # call onSuccess function if it is given and callable
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
            record.addListOfEntries(entries)  # add entries to the record
            tpm.updatePIDRecord(record)  # update PID record in the Typed PID-Maker
            if onSuccess is not None and callable(
                onSuccess
            ):  # call onSuccess function if it is given and callable
                onSuccess(record.getPID())
            return record.getPID()
        elif record.entryExists(
            "21.T11148/b8457812905b83046284", cleartext_presumed_pid
        ):  # The value of digitalObjectLocation matches the presumed PID
            logger.debug(
                f"Adding entries to existing record with PID {presumed_pid}. Identified by digitalObjectLocation.",
                entries,
            )
            record.addListOfEntries(entries)  # add entries to the record
            tpm.updatePIDRecord(record)  # update PID record in the Typed PID-Maker
            if onSuccess is not None and callable(onSuccess):
                onSuccess(record.getPID())
            return record.getPID()

    # Check if the PID record exists in Elasticsearch and get it from the Typed PID-Maker
    try:
        logger.info(
            "Couldn't find a record to add entries to. Calling addEntries function. Starting to search in elasticsearch."
        )
        pid = elasticsearch.searchForPID(
            cleartext_presumed_pid
        )  # search for the PID in Elasticsearch

        if (
            pid is not None
        ):  # PID found in Elasticsearch. This is the most probable match determined by Elasticsearch for the presumed PID
            logger.info(
                f"Found PID record in Elasticsearch with PID {pid}. Adding entries to it."
            )
            record = tpm.getPIDRecord(
                pid
            )  # get the PID record from the Typed PID-Maker with the found PID from Elasticsearch

            if record is not None and isinstance(
                record, PIDRecord
            ):  # Check if a PID record was found in the Typed PID-Maker
                record.addListOfEntries(entries)  # add entries to the record
                pid_records.append(
                    record
                )  # add the record to the list of PID records for future use
                tpm.updatePIDRecord(
                    record
                )  # update the PID record in the Typed PID-Maker
                if onSuccess is not None and callable(
                    onSuccess
                ):  # call onSuccess function if it is given and callable
                    onSuccess(
                        record.getPID()
                    )  # call onSuccess function with the PID of the record
                return record.getPID()
    except Exception as e:  # Something went wrong during the search in Elasticsearch or getting the PID record from the Typed PID-Maker
        if allowRetry:  # Retry is enabled -> add the entries to the list of future entries for future use
            future_entry = {"presumed_pid": presumed_pid, "entries": entries}
            logger.info(
                f"Could not find a PID record locally or in Elasticsearch with PID {presumed_pid} aka {cleartext_presumed_pid}. Reminding entry for future use. {str(e)}",
                future_entry,
                pid_records,
                e,
            )
            if (
                presumed_pid is not None or entries is not None
            ):  # Check if the presumed PID and the entries are not None
                future_entries.append(future_entry)
        else:  # Retry is disabled -> raise an exception
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

    return "None"  # No PID record found. Return "None"


async def create_pidRecords_from_resources(
    repo: AbstractRepository, resources: list[dict]
) -> None:
    """
    This function PID records for the given resources of the given repository.
    It extracts the PID records from the resources and adds them to the list of PID records to be created.
    If the repository FDO is not found, it is created and added to the list of PID records to be created.
    If an error occurs during the creation of the PID records, the error is added to the list of errors for further investigation by the user.

    Args:
        repo (AbstractRepository): The repository to create PID records for
        resources (list[dict]): A list of resources to create PID records from (e.g., JSON objects). The format of the resources is repository specific.

    Returns:
        list[PIDRecord]: A list of PID records created from the provided resources

    Raises:
        Exception: If an error occurs during the creation of the PID records
    """
    logger.info(f"Creating PID records for the {len(resources)} resources")

    # get repository FDO
    repo_FDO, isNew = await _getRepoFAIRDO(repo)

    # Extract PID records from the resources
    for resource in resources:  # iterate over the resources
        logger.debug(f"Extracting PID record from {str(resource)[:100]}")
        try:
            pid_record = await repo.extractPIDRecordFromResource(
                resource, addRelationship
            )  # extract PID record from the resource
            if pid_record is not None and isinstance(
                pid_record, PIDRecord
            ):  # Check if a PID record was extracted
                pid_record.addEntry(  # add the repository FDO as primary source to the PID record
                    "21.T11148/a753134738da82809fc1",
                    repo_FDO.getPID(),
                    "hadPrimarySource",
                )
                # repo_FDO.addEntry( TODO: add this entry to the repository FDO; disabled due to size constraints of Handle records (problems at ~400 KB; actual size of repository FDO: ~2.9 MB)
                #     "21.T11148/4fe7cde52629b61e3b82",
                #     pid_record.getPID(),
                #     "isMetadataFor",
                # )
                records_to_create.append(
                    pid_record
                )  # add the PID record to the list of PID records to be created
            else:  # No PID record extracted from the resource -> add an error to the list of errors
                logger.error(f"No PID record extracted from {resource}")
                errors.append(
                    {
                        "url": resource,
                        "error": "No PID record extracted",
                        "timestamp": datetime.now().isoformat(),
                    }
                )
        except Exception as e:  # An error occurred during the extraction of the PID record -> add an error to the list of errors
            logger.error(f"Error extracting PID record from {resource}: {str(e)}", e)
            errors.append(
                {
                    # "url": resource,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )

    logger.info("Dealing with future entries", future_entries)
    # Add entries from future entries to PID records (second attempt)
    while len(future_entries) > 0:  # while there are future entries
        entry = future_entries.pop()  # get the first entry
        try:  # try to add the entries to the PID record
            pid = entry["presumed_pid"]
            entries = entry["entries"]

            logger.info(
                f"Adding entries to PID record with PID {pid} from future entries (second attempt).",
                entries,
            )

            addRelationship(
                pid, entries, None, False
            )  # add the entries to the PID record (without retry to ensure that no infinite loop is created)
        except Exception as e:  # An error occurred during the addition of the entries to the PID record -> add an error to the list of errors
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

    if isNew:  # Check if the repository FDO is newly created
        logger.info(f"Creating repository FDO with preliminary PID {repo_FDO.getPID()}")
        records_to_create.append(
            repo_FDO
        )  # add the repository FDO to the list of PID records to be created
    else:  # The repository FDO is not newly created
        logger.info(f"Updating repository FDO with actual PID {repo_FDO.getPID()}")
        tpm.updatePIDRecord(
            repo_FDO
        )  # update the repository FDO in the Typed PID-Maker
        await elasticsearch.addPIDRecord(
            repo_FDO
        )  # add the repository FDO to Elasticsearch

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
    Create PID records from scratch for the given time frame or all available URLs if no time frame is given.

    Args:
        repos (list[AbstractRepository]): The repositories to create PID records for. If None, all available repositories are used.
        start (datetime): The start of the time frame. If None, all available URLs are used.
        end (datetime): The end of the time frame. If None, all available URLs are used.
        dryrun (bool): If true, the PID records will not be created in TPM or Elasticsearch

    Returns:
        list[PIDRecord]: A list of PID records created from scratch

    Raises:
        Exception: If an error occurs during the creation of the PID records
    """
    start_time = datetime.now()
    for repo in repos:  # iterate over the repositories
        resources = []  # list of resources to create PID records from

        # Get the URLs for the given time frame or all available URLs if no time frame is given
        if (  # Check if the start and end of the time frame are given and are of type datetime
            start is not None
            and end is not None
            and isinstance(start, datetime)
            and isinstance(end, datetime)
        ):
            resources = await repo.getResourcesForTimeFrame(
                start, end
            )  # get the resources for the given time frame
        else:
            with open("last_run_" + repo.repositoryID.replace("/", "_"), "w") as f:
                f.write(datetime.now().isoformat())
            resources = (
                await repo.getAllAvailableResources()
            )  # get all available resources

        logger.info(f"Creating PID records from scratch for {len(resources)} resources")

        await create_pidRecords_from_resources(
            repo, resources
        )  # create PID records from the resources

    real_pid_records = await _createRecordsToCreate(
        dryrun
    )  # create PID records in TPM and Elasticsearch

    # write PID records to file
    with open("pid_records_all.json", "w") as f:
        json.dump([record.toJSON() for record in pid_records], f)
        logger.info("PID records written to file pid_records.json")

    logger.info(
        f"Finished creating {len(real_pid_records)} PID records in {datetime.now() - start_time}"
    )
    return real_pid_records


async def add_all_existing_pidRecords_to_elasticsearch(fromFile: str = None) -> None:
    """
    Add all existing PID records to Elasticsearch. If fromFile is not None, the PID records will be read from a file instead of the Typed PID-Maker.

    Args:
        fromFile (bool): If true, the PID records will be read from a file instead of the Typed PID-Maker. Default is None.

    Raises:
        Exception: If an error occurs during the addition of the PID records to Elasticsearch
    """
    try:
        records: list[PIDRecord]
        if fromFile is not None:  # Check if the PID records should be read from a file
            with open(fromFile, "r") as f:  # read the PID records from the file
                records = [
                    PIDRecord.fromJSON(record) for record in json.load(f)
                ]  # convert the JSON objects to PID records
                logger.info(f"found {len(records)} PID records in file {fromFile}")
        else:  # Read the PID records from the Typed PID-Maker
            records = (
                await tpm.getAllPIDRecords()
            )  # get all PID records from the Typed PID-Maker
            logger.info(f"found {len(records)} PID records in TPM")

        logger.info("Adding all existing PID records to Elasticsearch")
        await elasticsearch.addPIDRecords(
            records
        )  # add the PID records to Elasticsearch

        with open("pid_records_all.json", "w") as f:  # write the PID records to a file
            json.dump([record.toJSON() for record in records], f)
            logger.info("PID records written to file pid_records_all.json")
    except (
        Exception
    ) as e:  # An error occurred during the addition of the PID records to Elasticsearch
        logger.error("Error adding PID records to Elasticsearch")
        raise Exception("Error adding PID records to Elasticsearch", e)


def _deduplicateListOfPIDRecords(input: list[PIDRecord]) -> list[PIDRecord]:
    """
    Deduplicate a list of PID records by merging the duplicates.

    Args:
        input (list[PIDRecord]): The list of PID records to deduplicate

    Returns:
        list[PIDRecord]: The deduplicated list of PID records
    """
    deduplicated_pid_records: dict[str, PIDRecord] = {}
    for record in input:  # iterate over the PID records
        if (
            record.getPID() in deduplicated_pid_records
        ):  # Check if the PID record is already in the list of deduplicated PID records
            logger.debug(f"Found duplicate PID {record.getPID()}", record)
            deduplicated_pid_records[record.getPID()].merge(
                record
            )  # merge the duplicate PID record with the existing PID record
        else:  # PID record is not in the list of deduplicated PID records
            deduplicated_pid_records[record.getPID()] = (
                record  # add the PID record to the list of deduplicated PID records
            )
    return list(
        deduplicated_pid_records.values()
    )  # return the list of deduplicated PID records


async def _createRecordsToCreate(dryrun: bool = False) -> list[PIDRecord]:
    """
    Create the PID records in TPM and Elasticsearch from the list of PID records to be created.
    This function also deduplicates the list of PID records to be created.
    The list of PID records to be created is a global variable and therefore hasn't to be passed as an argument.
    Reason for this is that the same list is also used for adding relationships to PID records.

    Args:
        dryrun (bool): If true, the PID records will not be created in TPM or Elasticsearch. Default is False.

    Returns:
        list[PIDRecord]: The PID records created in TPM and Elasticsearch

    """
    logger.info("Fixing duplicate PIDs")  # Fix duplicate PIDs
    deduplicated_records = _deduplicateListOfPIDRecords(
        records_to_create
    )  # Deduplicate PID records

    with open(
        "deduplicated_records_to_create.json", "w"
    ) as f:  # write the deduplicated PID records to a file
        json.dump(
            [record.toJSON() for record in deduplicated_records], f
        )  # convert the PID records to JSON objects and write them to the file
        logger.info("PID records written to file deduplicated_records_to_create.json")

    if (
        not dryrun
    ):  # Check if the PID records should be created in TPM and Elasticsearch
        # Create PID records in TPM
        real_pid_records = []
        try:
            logger.info("Creating PID records in TPM")
            real_pid_records = tpm.createMultipleFAIRDOs(
                deduplicated_records
            )  # create PID records in TPM
        except Exception as e:  # An error occurred during the creation of the PID records in TPM -> add an error to the list of errors
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
        except Exception as e:  # An error occurred during the addition of the PID records to Elasticsearch -> add an error to the list of errors
            logger.error(f"Error adding PID records to Elasticsearch: {str(e)}")
            errors.append(
                {"error": e.__repr__(), "timestamp": datetime.now().isoformat()}
            )
        return real_pid_records
    else:  # Dryrun is enabled -> do not create PID records in TPM or Elasticsearch
        logger.warning("Dryrun: Not creating PID records in TPM or Elasticsearch")
        pid_records.extend(
            deduplicated_records
        )  # add PID records to the list of PID records
        return deduplicated_records  # return the deduplicated PID records


async def _getRepoFAIRDO(repo: AbstractRepository):
    """
    Get the repository FAIR-DO for the given repository.
    If the repository FDO is not found, it is created and added to the list of PID records to be created.
    This repository FAIR-DO contains information about the repository and is used as a value for "primary source" in the PID records created from the resources of the repository.
    For example, the repository FDO contains information about the repository name, the repository URL (as digitalObjectLocation), and a preview image.

    Args:
        repo (AbstractRepository): The repository to get the repository FAIR-DO for

    Returns:
        PIDRecord: The repository FAIR-DO record
        bool: Whether the repository FDO is newly created (True) or has to be updated (False)
    """
    new_repo_FDO = (
        repo.getRepositoryFDO()
    )  # get the repository FDO content for the given repository
    try:  # Try to get the existing repository FAIR-DO from Elasticsearch
        existing_repo_FDO_pid = elasticsearch.searchForPID(
            new_repo_FDO.getPID()
        )  # search for the repository FDO in Elasticsearch

        if (
            existing_repo_FDO_pid is not None
        ):  # Check if the repository FDO was found in Elasticsearch
            existing_repo_FDO = tpm.getPIDRecord(
                existing_repo_FDO_pid
            )  # get the repository FDO from the Typed PID-Maker

            if (
                existing_repo_FDO is not None
            ):  # Check if the repository FDO was found in the Typed PID-Maker
                actual_repoFDO_pid = (
                    existing_repo_FDO.getPID()
                )  # get the PID of the repository FDO
                logger.info(
                    f"Found existing repository FDO with PID {actual_repoFDO_pid}"
                )
                existing_repo_FDO.merge(
                    new_repo_FDO
                )  # merge possible changes to the repository FDO
                return (
                    existing_repo_FDO,
                    False,
                )  # return the existing repository FDO and that it is not newly created
    except Exception as e:  # An error occurred during the search for the repository FDO in Elasticsearch or getting the repository FDO from the Typed PID-Maker
        logger.info(f"Error getting existing repository FDO: {str(e)}")

    logger.info(f"Creating repository FDO with PID {new_repo_FDO.getPID()}")
    return (
        new_repo_FDO,
        True,
    )  # return the new repository FDO and that it is newly created, if the repository FDO was not found.


def extractBiggestFAIRDO(records: list[PIDRecord]) -> PIDRecord | None:
    """
    Extract the biggest FAIR-DO from a list of PID records.
    The biggest FAIR-DO is the PID record with the most entries (even if they are from the same data type).

    Args:
        records (list[PIDRecord]): The list of PID records to extract the biggest FAIR-DO from

    Returns:
        PIDRecord: The biggest FAIR-DO record from the list of PID records
    """
    if (
        not records
        or records is None
        or not isinstance(records, list)
        or len(records) == 0
    ):
        logger.error("Invalid input: records is None or not a list or empty")
        return None
    elif len(records) == 1:  # Only one PID record in the list
        logger.info("Only one PID record in list. Returning it.")
        return records[0]

    biggest_FDO = records[0]
    biggest_FDO_attributeValue = 0
    for record in records:  # iterate over the PID records
        for entry in record.getEntries().values():
            if isinstance(entry, list) and len(entry) > biggest_FDO_attributeValue:
                logger.info(f"New biggest FAIR-DO: {record.getPID()}")
                biggest_FDO = record
                biggest_FDO_attributeValue = len(entry)

    logger.info(f"Found biggest FAIR-DO: {biggest_FDO.getPID()}")
    return biggest_FDO


def extractRecordWithMostDataTypes(records: list[PIDRecord]) -> PIDRecord | None:
    """
    Extract the PID record with the most different data types from a list of PID records.
    The PID record with the most different data types is the PID record with the most different keys in its entries.

    Args:
        records (list[PIDRecord]): The list of PID records to extract the PID record with the most different data types from

    Returns:
        PIDRecord: The PID record with the most different data types from the list of PID records
    """
    if (
        not records
        or records is None
        or not isinstance(records, list)
        or len(records) == 0
    ):
        logger.error("Invalid input: records is None or not a list or empty")
        return None
    elif len(records) == 1:  # Only one PID record in the list
        logger.info("Only one PID record in list. Returning it.")
        return records[0]

    most_informative_FDO = records[0]
    for record in records:  # iterate over the PID records
        if len(record.getEntries()) > len(most_informative_FDO.getEntries()):
            logger.info(f"New most informative FAIR-DO: {record.getPID()}")
            most_informative_FDO = record

    logger.info(f"Found most informative FAIR-DO: {most_informative_FDO.getPID()}")
    return most_informative_FDO


if __name__ == "__main__":
    """
    Main function to extract the most informative FAIR-DO and the biggest FAIR-DO from a list of PID records.
    It reads the PID records from a file and writes the most informative FAIR-DO and the biggest FAIR-DO to files.
    """
    with open("pid_records_all.json", "r") as r:  # read the PID records from the file
        pid_records = [
            PIDRecord.fromJSON(record) for record in json.load(r)
        ]  # convert the JSON objects to PID records
        logger.info(f"found {len(pid_records)} PID records in file")

        # Extract the most informative FAIR-DO
        most_informative_FDO = extractRecordWithMostDataTypes(pid_records)
        with open("most_informative_FDO.json", "w") as f2:
            json.dump(most_informative_FDO.toJSON(), f2)
            logger.info(
                "Most informative FAIR-DO written to file most_informative_FDO.json"
            )

        # Extract the biggest FAIR-DO
        biggest_FDO = extractBiggestFAIRDO(pid_records)
        with open("biggest_FDO.json", "w") as f2:
            json.dump(biggest_FDO.toJSON(), f2)
            logger.info("Biggest FAIR-DO written to file biggest_FDO.json")
