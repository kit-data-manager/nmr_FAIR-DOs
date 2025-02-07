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
from nmr_FAIR_DOs.utils import (
    fetch_data,
    encodeInBase64,
    fetch_multiple,
    parseDateTime,
    parseSPDXLicenseURL,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class ChemotionRepository(AbstractRepository):
    """
    The ChemotionRepository class represents a repository to extract FAIR-DOs from the Chemotion repository (https://chemotion-repository.net).

    It implements the AbstractRepository class and defines the methods to extract FAIR-DOs from the Chemotion repository.

    Attributes:
        _baseURL (str): The base URL of the Chemotion repository. E.g. "https://chemotion-repository.net".
        _limit (int): The number of records to fetch in one request. Default is 1000.
    """

    _baseURL: str

    def __init__(self, baseURL: str, limit: int = 1000):
        """
        Constructor for the ChemotionRepository class.

        Args:
            baseURL (str): The base URL of the Chemotion repository. E.g. "https://chemotion-repository.net".
            limit (int): The number of records to fetch in one request. Default is 500.

        """
        if not baseURL or baseURL == "":  # Check if the base URL is empty
            raise ValueError("Base URL cannot be empty.")

        if (
            not limit or limit is None or not isinstance(limit, int) or limit <= 0
        ):  # Check if the limit is a positive integer
            raise ValueError("Limit must be a positive integer.")

        self._baseURL = baseURL
        self._limit = limit
        logger.debug(
            f"Created ChemotionRepository with baseURL: {baseURL} and limit: {limit}"
        )

    @property
    def repositoryID(self) -> str:
        return "Chemotion_" + self._baseURL

    async def getResourcesForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[dict]:
        urls = await self._getAllURLs(
            start, end
        )  # Get all URLs for the specified time frame
        return await fetch_multiple(urls)  # Fetch all resources from the URLs

    async def extractPIDRecordFromResource(
        self,
        resource: dict,
        add_relationship: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord | None:
        if (
            not resource
            or resource is None
            or resource == ""
            or not isinstance(resource, dict)
        ):  # Check if the resource is empty or not a dict
            raise ValueError("Resource cannot be empty and must be a dict.")

        if (
            not add_relationship
            or add_relationship is None
            or add_relationship == ""
            or not callable(add_relationship)
        ):  # Check if the add_relationship function is empty or not a function
            raise ValueError("add_relationship function cannot be empty.")

        logger.debug(
            f"Extracted resource from Chemotion repository: {str(resource)[0:100]}"
        )

        if resource["@type"] == "Dataset":  # Check if the resource is a dataset
            return await self._mapDataset2PIDRecord(
                resource
            )  # Map the dataset to a PID record
        elif resource["@type"] == "Study":  # Check if the resource is a study
            return await self._mapStudy2PIDRecord(
                resource, add_relationship
            )  # Map the study to a PID record
        else:
            raise ValueError(
                "Invalid resource from Chemotion repository."
            )  # Raise an error if the resource is not a dataset or study

    async def _getAllURLs(self, start: datetime, end: datetime):
        urls = []
        urls.extend(
            await self._getURLsForCategory("Container", start, end)
        )  # Get all URLs for the category "Container"
        urls.extend(
            await self._getURLsForCategory("Sample", start, end)
        )  # Get all URLs for the category "Sample"
        return urls

    async def _getURLsForCategory(
        self, category: str, start: datetime, end: datetime
    ) -> list[str]:
        """
        Get all URLs for a specific category and time frame.

        Args:
            category (str): The category to fetch the URLs for. Either "Sample" or "Container".
            start (datetime): The start of the time frame.
            end (datetime): The end of the time frame.

        Returns:
            list[str]: A list of URLs for the specified category and time frame.

        Raises:
            ValueError: If the category is invalid or the start and end date are invalid
        """
        if (
            not start
            or not end
            or start == ""
            or end == ""
            or start is None
            or end is None
            or not isinstance(start, datetime)
            or not isinstance(end, datetime)
        ):  # Check if the start and end date are empty or not a datetime
            raise ValueError(
                "Start date and end date cannot be empty and must be a datetime."
            )
        if start > end:  # Check if the start date is before the end date
            raise ValueError("Start date must be before end date.")
        if start > datetime.now():  # Check if the start date is in the future
            raise ValueError("Start date must be in the past.")

        if (
            not category or category == "" or category not in ["Sample", "Container"]
        ):  # Check if the category is empty or not "Sample" or "Container"
            raise ValueError(
                "Category cannot be empty and must be either 'Sample' or 'Container' ."
            )

        # Create the URL template
        url_template = Template(
            "$repositoryURL/api/v1/public/metadata/publications?type=$category&offset=$offset&limit=$limit&date_from=$dateFrom&date_to=$dateTo"
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
                category=category,
                dateFrom=f"{start.year}-{start.month}-{start.day}",
                dateTo=f"{end.year}-{end.month}-{end.day}",
            )
            logger.debug("Getting frame " + url)

            # Fetch the data
            response = await fetch_data(url, True)
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
    def _extractContactField(field_name: str, json_object: dict) -> list[str]:
        """
        Extracts contacts from a field in a JSON object.

        Args:
            field_name (str): The name of the field to extract the contacts from.
            json_object (dict): The JSON object to extract the contacts from.

        Returns:
            list[str]: A list of contact identifiers extracted from the field.
        """
        contacts = []

        def extractContact(contact_element: dict) -> str | None:
            """
            Extracts the identifier of a contact from a contact object.

            Args:
                contact_element (dict): The contact JSON object to extract the identifier from.

            Returns:
                str: The identifier of the contact
            """
            if "identifier" in contact_element:
                logger.debug(
                    f"Found identifier in identifier field {contact_element['identifier']}"
                )
                return contact_element[
                    "identifier"
                ]  # get the identifier of the contact from the identifier field if it exists
            elif "@id" in contact_element:
                logger.debug(f"Found identifier in @id field {contact_element['@id']}")
                return contact_element[
                    "@id"
                ]  # get the identifier of the contact from the @id field if it exists
            return None

        if field_name in json_object:
            field = json_object[
                field_name
            ]  # get the field e.g. author, creator, contributor

            if isinstance(field, list):  # if the field is a list of contacts
                for element in field:  # iterate over the contacts
                    logger.debug(
                        f"Extracting contact from {field_name} out of list", element
                    )
                    identifier = extractContact(
                        element
                    )  # extract the identifier of the contact
                    if identifier not in contacts and identifier is not None:
                        logger.debug(f"Adding contact {identifier} to contacts")
                        contacts.append(identifier)
                    else:
                        logger.debug(
                            f"Contact {identifier} already in contacts", contacts
                        )

            elif isinstance(field, dict):  # if the field is a single contact
                logger.debug(f"Extracting contact from {field_name} out of dict", field)
                identifier = extractContact(
                    field
                )  # extract the identifier of the contact
                if identifier not in contacts and identifier is not None:
                    logger.debug(f"Adding contact {identifier} to contacts")
                    contacts.append(identifier)
                else:
                    logger.debug(
                        f"Contact {identifier} already in contacts or is None",
                        contacts,
                    )
            else:
                logger.debug(f"Field {field_name} is not a list or dict", field)
        else:
            logger.debug(f"Field {field_name} not found in json", json_object)

        logger.debug(f"Extracted contacts from {field_name}", contacts)
        return contacts

    @staticmethod
    def _mapGenericInfo2PIDRecord(chemotion_content) -> PIDRecord:
        """
        Maps generic information to a PID record.

        Args:
            chemotion_content (dict): The JSON response from the Chemotion API.

        Returns:
            PIDRecord: The PID record mapped from the generic information
        """
        logger.debug(f"Mapping generic info to PID Record: {chemotion_content['@id']}")

        fdo = PIDRecord(
            encodeInBase64(chemotion_content["@id"].replace("https://doi.org/", ""))
        )

        fdo.addEntry(
            "21.T11148/076759916209e5d62bd5",
            "21.T11148/b9b76f887845e32d29f7",  # TODO: get the correct KIP PID; currently HelmholtzKIP
            "Kernel Information Profile",
        )

        fdo.addEntry(
            "21.T11148/1c699a5d1b4ad3ba4956",
            "21.T11148/ca9fd0b2414177b79ac2",
            "digitalObjectType",
        )

        fdo.addEntry(
            "21.T11148/b8457812905b83046284",
            f"https://dx.doi.org/{chemotion_content["@id"].replace("https://doi.org/", "")}",
            "digitalObjectLocation",
        )

        # Generate a list of contacts from the author, creator, and contributor fields
        contact = []
        contact.extend(
            ChemotionRepository._extractContactField("author", chemotion_content)
        )
        contact.extend(
            ChemotionRepository._extractContactField("creator", chemotion_content)
        )
        contact.extend(
            ChemotionRepository._extractContactField("contributor", chemotion_content)
        )
        logger.debug(f"Found {len(contact)} contacts")

        for (
            contact_id
        ) in contact:  # Iterate over the contacts and add them to the PID record
            fdo.addEntry(
                "21.T11148/1a73af9e7ae00182733b",
                "https://orcid.org/" + contact_id,
                "contact",
            )

        if (
            "dateModified" in chemotion_content
            and chemotion_content["dateModified"] is not None
        ):  # Add the dateModified to the PID record if it exists
            fdo.addEntry(
                "21.T11148/397d831aa3a9d18eb52c",
                parseDateTime(chemotion_content["dateModified"]).isoformat(),
                "dateModified",
            )

        if (
            "dateCreated" in chemotion_content
            and chemotion_content["dateCreated"] is not None
        ):  # Add the dateCreated to the PID record if it exists
            fdo.addEntry(
                "21.T11148/aafd5fb4c7222e2d950a",
                parseDateTime(chemotion_content["dateCreated"]).isoformat(),
                "dateCreated",
            )

        logger.debug(f"Mapped generic info to FAIR-DO: {fdo.getPID()}")
        return fdo

    @staticmethod
    async def _mapDataset2PIDRecord(dataset) -> PIDRecord:
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
            fdo = ChemotionRepository._mapGenericInfo2PIDRecord(
                dataset
            )  # Start with the generic information

            fdo.addEntry("21.T11969/b736c3898dd1f6603e2c", "Dataset", "resourceType")

            fdo.addEntry("21.T11148/6ae999552a0d2dca14d6", dataset["name"], "name")

            fdo.addEntry(
                "21.T11969/8710d753ad10f371189b", dataset["url"], "landingPageLocation"
            )

            fdo.addEntry(
                "21.T11148/f3f0cbaa39fa9966b279", dataset["identifier"], "identifier"
            )

            if (
                "measurementTechnique" in dataset
            ):  # Add the measurement technique to the PID record if it exists
                fdo.addEntry(
                    "21.T11969/7a19f6d5c8e63dd6bfcb",
                    dataset["measurementTechnique"]["@id"],
                    "NMR method",
                )

            fdo.addEntry(  # Add the license to the PID record
                "21.T11148/2f314c8fe5fb6a0063a8",
                await parseSPDXLicenseURL(dataset["license"]),
                "license",
            )

            if "isPartOf" in dataset and not fdo.entryExists(
                "21.T11148/aafd5fb4c7222e2d950a"
            ):
                if (
                    "dateCreated" in dataset["isPartOf"]
                ):  # Add the dateCreated of the parent dataset to the PID record if fdo does not already contain a dateCreated
                    fdo.addEntry(
                        "21.T11148/aafd5fb4c7222e2d950a",
                        parseDateTime(dataset["isPartOf"]["dateCreated"]).isoformat(),
                        "dateCreated",
                    )
                elif (
                    "datePublished" in dataset["isPartOf"]
                ):  # Add the datePublished of the parent dataset to the PID record if fdo does not already contain a dateCreated
                    fdo.addEntry(
                        "21.T11148/aafd5fb4c7222e2d950a",
                        parseDateTime(dataset["isPartOf"]["datePublished"]).isoformat(),
                        "dateCreated",
                    )

            return fdo
        except Exception as e:
            logger.error("Error mapping dataset to FAIR-DO", dataset, e)
            raise e

    @staticmethod
    async def _mapStudy2PIDRecord(
        study,
        addRelationship: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord:
        """
        Maps a study to a PID record.

        Args:
            study (dict): The study to map to a PID record. This is the JSON response from the Chemotion API.
            addRelationship (function): The function to add entries to a PIDRecord. This function expects the following arguments in the following order: (str, list[PIDRecordEntry]) and returns a str. The first argument is the (presumed) PID of the target record, the second argument is a list of entries to add to the target record. It returns the PID of the target record.

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

            fdo.addEntry("21.T11969/b736c3898dd1f6603e2c", "Study", "resourceType")

            fdo.addEntry(
                "21.T11148/2f314c8fe5fb6a0063a8",
                await parseSPDXLicenseURL(study["includedInDataCatalog"]["license"]),
                "license",
            )

            if (
                "about" not in study
                or not isinstance(study["about"], list)
                or len(study["about"]) == 0
            ):  # Check if the study contains any datasets
                raise ValueError("Study does not contain any datasets", study)

            for entry in study["about"]:  # Iterate over the datasets in the study
                if "image" in entry:  # Add the image to the PID record if it exists
                    fdo.addEntry(
                        "21.T11148/7fdada5846281ef5d461",
                        entry["image"],
                        "locationPreview",
                    )

                if "hasBioChemEntityPart" in entry:
                    parts = entry["hasBioChemEntityPart"]
                    if not isinstance(parts, list) or isinstance(parts, dict):
                        parts = [parts]

                    for (
                        part
                    ) in parts:  # Iterate over the parts of the dataset if they exist
                        value = {}
                        if (
                            "molecularWeight" in part
                            and "value" in part["molecularWeight"]
                            and part["molecularWeight"]["value"] is not None
                        ):  # add the molecular weight to the characterized compound if it exists
                            value["21.T11969/6c4d3deac9a49b65886a"] = float(
                                part["molecularWeight"]["value"]
                            )
                        if (
                            "url" in part and part["url"] is not None
                        ):  # add the PubChem URL to the characterized compound if it exists
                            value["21.T11969/f9cb9b53273ce0da7739"] = part["url"]

                        if (
                            len(value) > 0
                        ):  # Add the characterized compound to the PID record if any values were found
                            fdo.addEntry(
                                "21.T11969/d15381199a44a16dc88d",
                                value,  # This is a dictionary of the values found
                                "characterizedCompound",
                            )
                        else:
                            logger.warning(
                                f"The provided part does not contain a molecularWeight or url: {part}"
                            )
                if "name" in entry:  # Add the name to the PID record if it exists
                    fdo.addEntry(
                        "21.T11148/6ae999552a0d2dca14d6", entry["name"], "name"
                    )
                if "url" in entry:  # Add the URL to the PID record if it exists
                    fdo.addEntry(
                        "21.T11969/8710d753ad10f371189b",
                        entry["url"],
                        "landingPageLocation",
                    )
                if (
                    "identifier" in entry
                ):  # Add the identifier to the PID record if it exists
                    fdo.addEntry(
                        "21.T11148/f3f0cbaa39fa9966b279",
                        entry["identifier"],
                        "identifier",
                    )

                if "subjectOf" in entry:
                    for dataset in entry["subjectOf"]:  # Iterate over the datasets
                        presumedDatasetID = encodeInBase64(
                            dataset["@id"].replace("https://doi.org/", "")
                        )

                        datasetEntries = [  # Prepare the dataset entries
                            PIDRecordEntry(
                                "21.T11148/d0773859091aeb451528",
                                fdo.getPID(),
                                "hasMetadata",
                            )
                        ]

                        if (
                            not fdo.entryExists("21.T11148/aafd5fb4c7222e2d950a")
                            and "dateCreated" in dataset
                        ):  # Add the dateCreated to the PID record if it does not already exist but is found in the dataset
                            fdo.addEntry(
                                "21.T11148/aafd5fb4c7222e2d950a",
                                parseDateTime(dataset["dateCreated"]).isoformat(),
                                "dateCreated",
                            )

                        if fdo.entryExists(
                            "21.T11148/7fdada5846281ef5d461"
                        ):  # Add the images to the dataset entries if they exist
                            images = fdo.getEntry("21.T11148/7fdada5846281ef5d461")
                            logger.debug(f"Found images in study {images}")
                            if images is not None and isinstance(images, list):
                                datasetEntries.extend(images)
                            elif images is not None and isinstance(
                                images, PIDRecordEntry
                            ):
                                datasetEntries.append(images)

                        if fdo.entryExists(
                            "21.T11969/d15381199a44a16dc88d"
                        ):  # Add the compounds to the dataset entries if they exist
                            compounds = fdo.getEntry("21.T11969/d15381199a44a16dc88d")
                            logger.debug(f"Found compounds in study {compounds}")
                            if compounds is not None and isinstance(compounds, list):
                                datasetEntries.extend(compounds)
                            elif compounds is not None and isinstance(
                                compounds, PIDRecordEntry
                            ):
                                datasetEntries.append(compounds)

                        try:  # Add the dataset reference to the study

                            def add_metadata_entry(fdo_pid: str, pid: str) -> None:
                                """
                                Adds a metadata entry to the study.

                                Args:
                                    fdo_pid (str): The PID of the study.
                                    pid (str): The PID of the dataset.

                                Returns:
                                    None
                                """
                                if pid is not None:  # Ensure the PID is not None
                                    addRelationship(
                                        fdo_pid,  # Add the relationship between the study and the dataset
                                        [
                                            PIDRecordEntry(  # Add the relationship entry
                                                "21.T11148/4fe7cde52629b61e3b82",
                                                pid,
                                                "isMetadataFor",
                                            )
                                        ],
                                        None,  # No callback function
                                    )

                            addRelationship(  # Add the dataset entries to the dataset
                                presumedDatasetID,  # presumed PID of the dataset
                                datasetEntries,  # dataset entries as defined above
                                lambda pid: add_metadata_entry(
                                    fdo.getPID(), pid
                                ),  # callback function to add the dataset reference to the study after the relationship has been added
                            )
                        except Exception as e:  # Log an error if the dataset reference could not be added to the study
                            logger.error(
                                "Error adding dataset reference to study",
                                presumedDatasetID,
                                datasetEntries,
                                e,
                            )

            return fdo
        except Exception as e:
            print("Error mapping study to FAIR-DO", study, e)
            raise e

    def getRepositoryFDO(self) -> PIDRecord:
        fdo = PIDRecord(encodeInBase64(self._baseURL))
        fdo.addEntry(
            "21.T11148/076759916209e5d62bd5",
            "21.T11148/b9b76f887845e32d29f7",  # TODO: get the correct KIP PID; currently HelmholtzKIP
            "Kernel Information Profile",
        )
        fdo.addEntry(
            "21.T11148/1c699a5d1b4ad3ba4956",
            "21.T11148/010acb220a9c2c8c0ee6",  # TODO: text/html for now
            "digitalObjectType",
        )

        fdo.addEntry(
            "21.T11148/b8457812905b83046284",
            self._baseURL,
            "digitalObjectLocation",
        )

        fdo.addEntry(
            "21.T11969/8710d753ad10f371189b",
            self._baseURL,
            "landingPageLocation",
        )

        fdo.addEntry(
            "21.T11148/aafd5fb4c7222e2d950a",
            datetime.now().isoformat(),
            "dateCreated",
        )

        fdo.addEntry(
            "21.T11148/6ae999552a0d2dca14d6",
            "Chemotion Repository",
            "name",
        )

        fdo.addEntry(
            "21.T11148/7fdada5846281ef5d461",
            "https://www.chemotion-repository.net/images/repo/Chemotion-V1.png",
            "locationPreview",
        )

        fdo.addEntry("21.T11969/b736c3898dd1f6603e2c", "Repository", "resourceType")

        return fdo
