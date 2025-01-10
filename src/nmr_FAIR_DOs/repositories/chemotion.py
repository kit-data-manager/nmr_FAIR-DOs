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
from datetime import datetime
from string import Template
from typing import Callable

from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository
from nmr_FAIR_DOs.utils import fetch_data

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


class ChemotionRepository(AbstractRepository):
    _baseURL: str

    def __init__(self, baseURL: str, limit: int = 500):
        """
        Constructor for the ChemotionRepository class.

        Args:
            baseURL (str): The base URL of the Chemotion repository. E.g. "https://chemotion-repository.net".
            Limit (int): The number of records to fetch in one request. Default is 500.

        """
        if not baseURL or baseURL == "":
            raise ValueError("Base URL cannot be empty.")

        if not limit or limit is None or not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        self._baseURL = baseURL
        self._limit = limit
        logger.debug(
            f"Created ChemotionRepository with baseURL: {baseURL} and limit: {limit}"
        )

    @property
    def repositoryID(self) -> str:
        return "Chemotion_" + self._baseURL

    async def listAvailableURLs(self) -> list[str] | None:
        return await self.listURLsForTimeFrame(datetime.min, datetime.now())

    async def listURLsForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[str] | None:
        return await self._getAllURLs(start, end)

    async def extractPIDRecordFromResource(
        self, url: str, addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> PIDRecord | None:
        logger.debug(f"Extracting PID record from {url}", addEntries)

        if not url or url == "" or url is None:
            raise ValueError("URL cannot be empty.")

        response = await fetch_data(url)
        if not response or response is None or not isinstance(response, dict):
            raise ValueError("Invalid response from Chemotion repository.")
        logger.debug("Extracted response from Chemotion repository", response)

        if response["@type"] == "Dataset":
            return self._mapDataset2PIDRecord(response)
        elif response["@type"] == "Study":
            return self._mapStudy2PIDRecord(response, addEntries)
        else:
            raise ValueError("Invalid response from Chemotion repository.")

    async def _getAllURLs(self, start: datetime, end: datetime):
        if (
            not start
            or not end
            or start == ""
            or end == ""
            or start is None
            or end is None
            or not isinstance(start, datetime)
            or not isinstance(end, datetime)
        ):
            raise ValueError(
                "Start date and end date cannot be empty and must be a datetime."
            )
        if start > end:
            raise ValueError("Start date must be before end date.")
        if start > datetime.now():
            raise ValueError("Start date must be in the past.")

        url_template = Template(
            "$repositoryURL/api/v1/public/metadata/publications?type=Sample&offset=$offset&limit=$limit&date_from=$dateFrom&date_to=$dateTo"
        )
        offset = 0
        complete = False
        urls = []

        while not complete:  # Loop until all entries are fetched
            # Create the URL
            url = url_template.safe_substitute(
                repositoryURL=self._baseURL,
                offset=offset,
                limit=self._limit,
                dateFrom=f"{start.year}-{start.month}-{start.day}",
                dateTo=f"{end.year}-{end.month}-{end.day}",
            )
            logger.debug("Getting frame " + url)

            # Fetch the data
            response = await fetch_data(url)
            if (
                not response
                or response is None
                or not isinstance(response, dict)
                or "publications" not in response
            ):
                raise ValueError("Invalid response from Chemotion repository.")

            # Add the URLs to the list
            urls.extend(response["publications"])

            if len(response["publications"]) != 0:
                offset += (
                    self._limit
                )  # Increase the offset by the limit to fetch the next frame of urls
            else:
                complete = True  # If no more entries are found, stop the loop

        # Log the number of URLs found and return them
        logger.info(f"found {len(urls)} urls\n\n")
        return urls

    @staticmethod
    def _mapGenericInfo2PIDRecord(json) -> PIDRecord:
        """
        Maps generic information to a PID record.

        Args:
            json (dict): The JSON response from the Chemotion API.

        Returns:
            PIDRecord: The PID record mapped from the generic information
        """
        logger.debug("Mapping generic info to PID Record", json["@id"])

        fdo = PIDRecord(json["@id"])

        fdo.addEntry(
            "21.T11148/076759916209e5d62bd5",
            "21.T11148/b9b76f887845e32d29f7",  # TODO: get the correct KIP PID; currently HelmholtzKIP
            "Kernel Information Profile",
        )

        fdo.addEntry(
            "21.T11148/1c699a5d1b4ad3ba4956",
            "21.T11148/ca9fd0b2414177b79ac2",  # TODO: get the correct digitalObjectType; currently application/json
            "digitalObjectType",
        )

        fdo.addEntry(
            "21.T11148/a753134738da82809fc1",
            json["publisher"][
                "url"
            ],  # TODO: Refer to FAIR-DO of the repository (via Handle PID)
            "hadPrimarySource",
        )

        fdo.addEntry(
            "21.T11148/b8457812905b83046284", json["@id"], "digitalObjectLocation"
        )

        fdo.addEntry("21.T11969/a00985b98dac27bd32f8", "Dataset", "resourceType")

        contact = []
        if json["author"] is list:
            for author in json["author"]:
                identifier = None
                if "identifier" in author:
                    identifier = author["identifier"]
                elif "@id" in author:
                    identifier = author["@id"]
                if identifier not in contact:
                    contact.append(identifier)
        elif json["author"] is dict:
            identifier = None
            if "identifier" in json["author"]:
                identifier = json["author"]["identifier"]
            elif "@id" in json["author"]:
                identifier = json["author"]["@id"]
            if identifier not in contact:
                contact.append(identifier)

        if "creator" in json:
            if json["creator"] is list:
                for creator in json["creator"]:
                    identifier = None
                    if "identifier" in creator:
                        identifier = creator["identifier"]
                    elif "@id" in creator:
                        identifier = creator["@id"]
                    if identifier not in contact:
                        contact.append(identifier)
            elif json["creator"] is dict:
                identifier = None
                if "identifier" in json["creator"]:
                    identifier = json["creator"]["identifier"]
                elif "@id" in json["creator"]:
                    identifier = json["creator"]["@id"]
                if identifier not in contact:
                    contact.append(identifier)
        if "contributor" in json:
            if json["contributor"] is list:
                for contributor in json["contributor"]:
                    identifier = None
                    if "identifier" in contributor:
                        identifier = contributor["identifier"]
                    elif "@id" in contributor:
                        identifier = contributor["@id"]
                    if identifier not in contact:
                        contact.append(identifier)
            elif json["contributor"] is dict:
                identifier = None
                if "identifier" in json["contributor"]:
                    identifier = json["contributor"]["identifier"]
                elif "@id" in json["contributor"]:
                    identifier = json["contributor"]["@id"]
                if identifier not in contact:
                    contact.append(identifier)

        for contact_id in contact:
            fdo.addEntry(
                "21.T11148/1a73af9e7ae00182733b",
                "https://orcid.org/" + contact_id,
                "contact",
            )

        if "dateModified" in json:
            fdo.addEntry(
                "21.T11148/397d831aa3a9d18eb52c", json["dateModified"], "dateModified"
            )

        if "dateCreated" in json:
            fdo.addEntry(
                "21.T11148/aafd5fb4c7222e2d950a", json["dateCreated"], "dateCreated"
            )

        logger.debug("Mapped generic info to FAIR-DO", json["@id"], fdo)
        return fdo

    @staticmethod
    def _mapDataset2PIDRecord(dataset) -> PIDRecord:
        """
        Maps a dataset to a PID record.

        Args:
            dataset (dict): The dataset to map to a PID record. This is the JSON response from the Chemotion API.

        Returns:
            PIDRecord: The PID record mapped from the dataset

        Raises:
            ValueError: If the provided data is not a dataset or is invalid
        """
        if "@type" not in dataset or dataset["@type"] != "Dataset":
            raise ValueError(
                "Bad Request - The provided data is not a dataset", dataset
            )

        logger.info("mapping dataset to FAIR-DO", dataset["@id"])
        try:
            fdo = ChemotionRepository._mapGenericInfo2PIDRecord(dataset)

            fdo.addEntry("21.T11148/6ae999552a0d2dca14d6", dataset["name"], "name")

            fdo.addEntry(
                "21.T11148/8710d753ad10f371189b", dataset["url"], "landingPageLocation"
            )

            fdo.addEntry(
                "21.T11148/f3f0cbaa39fa9966b279", dataset["identifier"], "identifier"
            )

            fdo.addEntry(
                "21.T11969/7a19f6d5c8e63dd6bfcb",
                dataset["measurementTechnique"]["@id"],
                "NMR method",
            )

            fdo.addEntry(
                "21.T11148/2f314c8fe5fb6a0063a8", dataset["license"], "license"
            )

            # entries.append({
            #     "key": "21.T11969/d15381199a44a16dc88d",
            #     "name": "characterizedCompound",
            #     "value": dataset["about"][0]["hasBioChemEntityPart"]["molecularWeight"]["value"]
            # }

            fdo.addEntry(
                "21.T11148/82e2503c49209e987740",
                "TODO",  # TODO: get the correct checksum
                "checksum",
            )

            return fdo
        except Exception as e:
            logger.error("Error mapping dataset to FAIR-DO", dataset, e)
            raise e

    @staticmethod
    def _mapStudy2PIDRecord(
        study, addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> PIDRecord:
        """
        Maps a study to a PID record.

        Args:
            study (dict): The study to map to a PID record. This is the JSON response from the Chemotion API.
            addEntries (function): The function to add entries to a PIDRecord. This function expects the following arguments in the following order: (str, list[PIDRecordEntry]) and returns a str. The first argument is the (presumed) PID of the target record, the second argument is a list of entries to add to the target record. It returns the PID of the target record.

        Returns:
            PIDRecord: The PID record mapped from the study

        Raises:
            ValueError: If the provided data is not a dataset or is invalid
        """

        if "@id" not in study or study["@type"] != "Study":
            raise ValueError("Bad Request - The provided data is not a study", study)

        logger.info("mapping study to FAIR-DO", study["@id"])

        try:
            fdo = ChemotionRepository._mapGenericInfo2PIDRecord(study)

            fdo.addEntry(
                "21.T11148/7fdada5846281ef5d461",
                study["about"][0]["image"],
                "locationPreview",
            )

            fdo.addEntry(
                "21.T11148/6ae999552a0d2dca14d6", study["about"][0]["name"], "name"
            )

            fdo.addEntry(
                "21.T11148/8710d753ad10f371189b",
                study["about"][0]["url"],
                "landingPageLocation",
            )

            fdo.addEntry(
                "21.T11148/2f314c8fe5fb6a0063a8",
                study["includedInDataCatalog"]["license"],
                "license",
            )

            fdo.addEntry(
                "21.T11969/d15381199a44a16dc88d",
                study["about"][0]["hasBioChemEntityPart"]["molecularWeight"]["value"],
                "characterizedCompound",
            )

            fdo.addEntry(
                "21.T11148/f3f0cbaa39fa9966b279",
                study["about"][0]["identifier"],
                "identifier",
            )

            fdo.addEntry(
                "21.T11148/82e2503c49209e987740",
                "TODO",  # TODO: get the correct checksum
                "checksum",
            )

            for dataset in study["about"][0]["subjectOf"]:
                presumedDatasetID = dataset["@id"]

                datasetEntries = [
                    PIDRecordEntry(
                        "21.T11148/d0773859091aeb451528", fdo.getPID(), "hasMetadata"
                    ),
                    PIDRecordEntry(
                        "21.T11969/d15381199a44a16dc88d",
                        study["about"][0]["hasBioChemEntityPart"]["molecularWeight"][
                            "value"
                        ],
                        "characterizedCompound",
                    ),
                ]

                datasetPID = addEntries(presumedDatasetID, datasetEntries)
                if datasetPID is not None:
                    fdo.addEntry(
                        "21.T11148/4fe7cde52629b61e3b82", datasetPID, "isMetadataFor"
                    )
                else:
                    logger.error(
                        "Error adding dataset reference to study",
                        datasetPID,
                        datasetEntries,
                    )

            return fdo
        except Exception as e:
            print("Error mapping study to FAIR-DO", study, e)
            raise e
