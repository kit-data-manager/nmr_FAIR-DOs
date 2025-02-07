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
from string import Template
from typing import Callable, Any

from nmr_FAIR_DOs.connectors.terminology import Terminology
from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository
from nmr_FAIR_DOs.utils import (
    encodeInBase64,
    fetch_data,
    parseDateTime,
    parseSPDXLicenseURL,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class NMRXivRepository(AbstractRepository):
    """
    This class creates FAIR-DOs for the contents of the NMRXiv repository. See https://nmrxiv.org/ for more information.
    The class is derived from the abstract class AbstractRepository.

    Attributes:
        _baseURL (str): The base URL of the NMRXiv repository.
        _terminology (Terminology): The terminology service used to map terms to ontology items.
        _fetch_fresh (bool): A flag indicating whether to fetch fresh data from the repository or use a cached version.
    """

    _baseURL: str

    def __init__(
        self, baseURL: str, terminology: Terminology, fetch_fresh: bool = True
    ) -> None:
        if baseURL is not None and isinstance(
            baseURL, str
        ):  # Check if the baseURL is valid
            self._baseURL = baseURL
        else:  # use the default baseURL
            self._baseURL = "https://nmrxiv.org"

        if terminology is not None and isinstance(
            terminology, Terminology
        ):  # Check if the terminology is valid
            self._terminology = terminology
        else:
            raise ValueError("Terminology must be an instance of Terminology")

        self._fetch_fresh = (
            fetch_fresh if fetch_fresh is not None else True
        )  # Set the fetch_fresh flag to the provided value or True if no value was provided

    @property
    def repositoryID(self) -> str:
        return "NMRXiv_" + self._baseURL

    async def getResourcesForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[dict]:
        result: list[dict] = []

        if not self._fetch_fresh:
            with open("nmrxiv_resources.json", "r") as r:
                result = json.load(r)  # Load the cached data
                if result is None or not isinstance(result, list):
                    logger.error("Invalid resources file. Fetching from scratch...")
                    self._fetch_fresh = (
                        True  # If the cached data is invalid, fetch fresh data
                    )
                else:
                    return result

        if (
            self._fetch_fresh or not os.path.isfile("nmrxiv_resources.json")
        ):  # Check if the data should be fetched fresh or if a cached version is not available
            result.extend(
                await self._getResourcesForCategory("datasets", start, end)
            )  # Fetch the datasets
            result.extend(
                await self._getResourcesForCategory("samples", start, end)
            )  # Fetch the samples
            result.extend(
                await self._getResourcesForCategory("projects", start, end)
            )  # Fetch the projects

            with open(
                "nmrxiv_resources.json", "w"
            ) as r:  # Write the fetched data to a file for caching. This is recommended since NMRXiv doesn't provide an API for getting just the URLs with a timestamp...
                json.dump(result, r)
        return result

    async def extractPIDRecordFromResource(
        self,
        resource: dict,
        add_relationship: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord | None:
        if (
            not resource or resource is None or not isinstance(resource, dict)
        ):  # Check if the resource is valid
            raise ValueError("Invalid resource.")
        elif (
            "original" not in resource or "bioschema" not in resource
        ):  # Check if the resource contains the original and bioschema data
            raise ValueError("Resource is missing original or bioschema data.")

        if "doi" not in resource["original"]:  # Check if the resource has a DOI
            raise ValueError("Resource has no DOI.")

        first_letter_type_indicator = resource[
            "original"
        ][
            "identifier"
        ].replace(
            "NMRXIV:", ""
        )[
            0
        ]  # Get the first letter of the identifier to determine the type of the resource

        if first_letter_type_indicator == "D":  # Check if the resource is a dataset
            return await self._mapDatasetToPIDRecord(resource)
        elif first_letter_type_indicator == "S":  # Check if the resource is a sample
            return await self._mapSampleToPIDRecord(resource, add_relationship)
        elif first_letter_type_indicator == "P":  # Check if the resource is a project
            return await self._mapProjectToPIDRecord(resource, add_relationship)
        else:  # If the resource is neither a dataset nor a sample nor a project, raise an error
            raise ValueError(
                "Resource is neither a dataset nor a sample nor a project.", resource
            )

    async def _getResourcesForCategory(
        self, category: str, start: datetime, end: datetime
    ) -> list[dict]:
        """
        Get all resources of the specified category that were created or updated in the specified time frame.

        Args:
            category (str): The category of the resources. Must be either "datasets" or "samples".
            start (datetime): The start date of the time frame.
            end (datetime): The end date of the time frame.

        Returns:
            list[dict]: A list of resources that were created or updated in the specified time frame.

        Raises:
            ValueError: If the category is invalid or the start or end date is invalid.
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
        ):  # Check if the start and end date are valid
            raise ValueError(
                "Start date and end date cannot be empty and must be a datetime."
            )
        if start > end:  # Check if the start date is before the end date
            raise ValueError("Start date must be before end date.")
        if start > datetime.now():  # Check if the start date is in the past
            raise ValueError("Start date must be in the past.")

        if (
            not category
            or category == ""
            or category not in ["datasets", "samples", "projects"]
        ):  # Check if the category is valid
            raise ValueError(
                "Category cannot be empty and must be either 'datasets' or 'samples' ."
            )

        # Remove the timezone information from the datetime objects
        start = start.replace(tzinfo=None)
        end = end.replace(tzinfo=None)

        # Create the URL
        url = f"{self._baseURL}/api/v1/list/{category}"
        complete = False
        objects: list[dict] = []

        while not complete:  # Loop until all entries are fetched
            # Create the URL
            logger.debug("Getting frame " + url)

            # Fetch the data
            response = await fetch_data(url, True)
            if (
                not response
                or response is None
                or not isinstance(response, dict)
                or "data" not in response
            ):  # Check if the response is valid
                raise ValueError("Invalid response from NMRXiv repository.")

            for elem in response["data"]:
                created = (
                    parseDateTime(elem["created_at"]).replace(tzinfo=None)
                    if "created_at" in elem
                    else None
                )  # Extract the creation date
                updated = (
                    parseDateTime(elem["updated_at"]).replace(tzinfo=None)
                    if "updated_at" in elem
                    else None
                )  # Extract the update date, if available

                try:
                    if created is None:  # This should never happen
                        logger.debug(f"Resource {elem['doi']} has no creation date.")
                        raise ValueError(
                            f"Resource {elem['doi']} has no creation date.", elem
                        )
                    elif (
                        start <= created <= end
                    ):  # Check if the creation date is in the timerange
                        logger.debug(
                            f"Creation date of the resource {elem['doi']} is in the timerange."
                        )
                        objects.append(
                            await self._getBioChemIntegratedDict(elem)
                        )  # add the resource to the list of objects to return
                    elif (
                        updated is not None and start <= updated <= end
                    ):  # Check if the update date is in the timerange (if available)
                        logger.debug(
                            f"Update date of the resource {elem['doi']} is in the timerange."
                        )
                        objects.append(
                            await self._getBioChemIntegratedDict(elem)
                        )  # add the resource to the list of objects to return
                    else:
                        logger.debug(f"Resource {elem['doi']} is not in the timerange.")
                        continue
                except (
                    Exception
                ) as e:  # Log the error and continue with the next resource
                    logger.error(
                        f"Error fetching BioSchema for resource {elem['doi']}: {str(e)}",
                        elem,
                        e,
                    )

            next_url = response["links"]["next"]  # Get the URL of the next page

            if (
                not next_url or next_url == "" or next_url == "null"
            ):  # Check if there are more pages by looking at the "next" link
                complete = True  # If there are no more pages, stop the loop
                logger.debug("Finished fetching all resources for " + category)
            else:
                url = next_url  # If there are more pages, get the next page

        # Log the number of URLs found and return them
        logger.info(f"found {len(objects)} urls\n")
        return objects

    async def _getBioChemIntegratedDict(self, elem: dict) -> dict:
        """
        Fetches the JSON-LD representation of the BioSchema for the specified ID.

        Args:
            elem (dict): The element to fetch the BioSchema for.

        Returns:
            dict: The JSON-LD representation of the BioSchema.

        Raises:
            ValueError: If the ID is invalid or the BioSchema cannot be fetched.
        """
        identifier = elem["identifier"].replace(
            "NMRXIV:", ""
        )  # Remove the NMRXIV: prefix from the identifier
        if not identifier or identifier == "" or not isinstance(identifier, str):
            raise ValueError("Invalid ID. Please provide a valid ID.", identifier, elem)

        template = Template("$repositoryURL/api/v1/schemas/bioschemas/$id")
        url = template.safe_substitute(repositoryURL=self._baseURL, id=identifier)
        logger.debug("Getting BioSchema JSON for " + url)

        bioschema = await fetch_data(url)  # Fetch the BioSchema JSON

        if not bioschema or bioschema is None or not isinstance(bioschema, dict):
            raise ValueError("Invalid BioSchema JSON.", bioschema, url)

        return {
            "original": self._removeDescription(
                elem
            ),  # Remove the description from the original data to save memory and have a cleaner output
            "bioschema": self._removeDescription(
                bioschema
            ),  # Remove the description from the BioSchema to save memory and have a cleaner output
        }

    @staticmethod
    async def _mapGenericInfo2PIDRecord(resource) -> PIDRecord:
        """
        Maps generic information to a PID record.

        Args:
            resource (dict): The JSON response from the NMRXiv API.

        Returns:
            PIDRecord: The PID record mapped from the generic information
        """
        try:
            original_resource = resource["original"]
            bioschema_resource = resource["bioschema"]

            logger.debug(
                f"Mapping generic info to PID Record: {original_resource['doi']}"
            )
            fdo = PIDRecord(encodeInBase64(original_resource["doi"]))

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

            if (
                "created_at" in original_resource
                and original_resource["created_at"] is not None
            ):  # Add the creation date to the PID record if available
                fdo.addEntry(
                    "21.T11148/aafd5fb4c7222e2d950a",
                    parseDateTime(original_resource["created_at"]).isoformat(),
                    "dateCreated",
                )

            if (
                "updated_at" in original_resource
                and original_resource["updated_at"] is not None
            ):  # Add the update date to the PID record if available
                fdo.addEntry(
                    "21.T11148/397d831aa3a9d18eb52c",
                    parseDateTime(original_resource["updated_at"]).isoformat(),
                    "dateModified",
                )

            if (
                "name" in original_resource
            ):  # Add the name of the resource to the PID record
                fdo.addEntry(
                    "21.T11148/6ae999552a0d2dca14d6", original_resource["name"], "name"
                )

            fdo.addEntry(
                "21.T11148/f3f0cbaa39fa9966b279",
                original_resource["doi"].replace("https://doi.org/", ""),
                "identifier",
            )

            if (
                "license" in original_resource
                and "spdx_id" in original_resource["license"]
                and original_resource["license"]["spdx_id"] is not None
            ):  # Add the license to the PID record if available
                fdo.addEntry(
                    "21.T11148/2f314c8fe5fb6a0063a8",
                    await parseSPDXLicenseURL(
                        original_resource["license"]["spdx_id"]
                    ),  # Get the SPDX URL for the license
                    "license",
                )
            elif (
                "license" in bioschema_resource
                and bioschema_resource["license"] is not None
            ):  # Add the license to the PID record if available
                fdo.addEntry(
                    "21.T11148/2f314c8fe5fb6a0063a8",
                    await parseSPDXLicenseURL(
                        bioschema_resource["license"]
                    ),  # Get the SPDX URL for the license
                    "license",
                )

            if "authors" in original_resource and isinstance(
                original_resource["authors"], list
            ):  # Add the authors to the PID record if available
                for author in original_resource["authors"]:
                    if "orcid_id" in author:
                        fdo.addEntry(
                            "21.T11148/1a73af9e7ae00182733b",
                            "https://orcid.org/"
                            + author["orcid_id"],  # Get the ORCiD URL
                            "contact",
                        )
                    elif "email" in author:
                        fdo.addEntry(
                            "21.T11148/e117a4a29bfd07438c1e",
                            author[
                                "email"
                            ],  # Add the email to the PID record if no ORCiD is available
                            "emailContact",
                        )
            elif (
                "owner" in original_resource and "email" in original_resource["owner"]
            ):  # Add the owner to the PID record if available and no authors are available
                fdo.addEntry(
                    "21.T11148/e117a4a29bfd07438c1e",
                    original_resource["owner"]["email"],
                    "emailContact",
                )
            elif (
                "users" in original_resource
            ):  # Add the users to the PID record if available and no authors or owners are available
                for user in original_resource["users"]:
                    if "email" in user:
                        fdo.addEntry(
                            "21.T11148/e117a4a29bfd07438c1e",
                            user["email"],
                            "emailContact",
                        )

            if (
                "download_url" in original_resource
                and original_resource["download_url"] is not None
            ):  # Add the download URL to the PID record if available (for samples and projects)
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_resource["download_url"],
                    "digitalObjectLocation",
                )
            else:  # Add the DOI to the PID record if no download URL is available
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    f"https://dx.doi.org/{original_resource['doi'].replace("https://doi.org/", "")}",
                    "digitalObjectLocation",
                )

            logger.debug(f"Mapped generic info to FAIR-DO: {fdo.getPID()}")
            return fdo
        except Exception as e:  # Log the error and raise it
            logger.error(f"Error mapping generic info to FAIR-DO: {str(e)}", resource)
            raise ValueError(
                f"Error mapping generic info to FAIR-DO: {str(e)}", resource
            )

    async def _mapDatasetToPIDRecord(self, dataset: dict) -> PIDRecord:
        """
        Maps a dataset to a PID record.

        Args:
            dataset (dict): The dataset to map to a PID record. Contains the original and BioSchema data.

        Returns:
            PIDRecord: The PID record mapped from the dataset.
        """
        # Extract the original and BioSchema data from the dataset
        original_dataset = dataset["original"]
        bioschema_dataset = dataset["bioschema"]

        if (
            not original_dataset
            or original_dataset is None
            or not isinstance(original_dataset, dict)
            or not original_dataset["identifier"].startswith("NMRXIV:D")
            or "@type" not in bioschema_dataset
            or bioschema_dataset["@type"] != "Dataset"
        ):  # Check if the dataset is valid
            raise ValueError(
                "Bad Request - The provided data is not a dataset", dataset
            )

        try:
            logger.info(f"mapping dataset to FAIR-DO: {bioschema_dataset["@id"]}")
            fdo = await self._mapGenericInfo2PIDRecord(
                dataset
            )  # Get the generic information for the dataset

            fdo.addEntry(
                "21.T11969/b736c3898dd1f6603e2c",
                "Dataset",
                "resourceType",
            )

            if "measurementTechnique" in bioschema_dataset and isinstance(
                bioschema_dataset["measurementTechnique"], dict
            ):  # Add the measurement technique to the PID record if available
                if "url" in bioschema_dataset["measurementTechnique"]:
                    fdo.addEntry(
                        "21.T11969/7a19f6d5c8e63dd6bfcb",
                        bioschema_dataset["measurementTechnique"]["url"],
                        "NMR method",
                    )
                else:
                    logger.info(
                        f"Measurement technique in entry {bioschema_dataset["@id"]} has no URL: {bioschema_dataset['measurementTechnique']}"
                    )

            if (
                "public_url" in original_dataset
                and original_dataset["public_url"] is not None
            ):  # Add the public URL to the PID record as a landing page if available
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    original_dataset["public_url"],
                    "landingPageLocation",
                )
            elif (
                "url" in bioschema_dataset and bioschema_dataset["url"] is not None
            ):  # Add the URL to the PID record as a landing page if available
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    bioschema_dataset["url"],
                    "landingPageLocation",
                )

            if (
                "dataset_photo_url" in original_dataset
                and original_dataset["dataset_photo_url"] is not None
            ):  # Add the dataset photo URL to the PID record as a preview if available
                fdo.addEntry(
                    "21.T11148/7fdada5846281ef5d461",
                    original_dataset["dataset_photo_url"],
                    "locationPreview",
                )

            if "variableMeasured" in bioschema_dataset and isinstance(
                bioschema_dataset["variableMeasured"], list
            ):
                for variable in bioschema_dataset[
                    "variableMeasured"
                ]:  # Iterate over the measured variables
                    try:
                        if (
                            "name" not in variable or "value" not in variable
                        ):  # Check if the variable has a name and a value
                            logger.warning(
                                f"Skipping variable {variable} because it has no name or value"
                            )
                            continue

                        name = variable["name"]
                        values = variable["value"]

                        if values is None:  # Check if the value is valid
                            logger.warning(
                                f"Skipping variable {name} because it has no value"
                            )
                            continue
                        elif not isinstance(values, list):
                            values = [values]

                        for value in values:  # Iterate over the values of the variable
                            if not isinstance(value, str):
                                logger.warning(
                                    f"Skipping variable {name} because value {value} is not a string"
                                )
                                continue
                            logger.debug(
                                f"Evaluating variable {name} with value {value}"
                            )

                            if (
                                name == "NMR solvent"
                            ):  # Check if the variable is the NMR solvent
                                ontology_item = await self._terminology.searchForTerm(
                                    value,
                                    "chebi",
                                    "http://purl.obolibrary.org/obo/CHEBI_197449",  # Has to be a child of "nmrSolvent"
                                )  # Search for the term in the ChEBI ontology with the terminology service
                                if (
                                    ontology_item is not None
                                ):  # Add the ontology item to the PID record if available
                                    fdo.addEntry(
                                        "21.T11969/92b4c6b461709b5b36f5",
                                        ontology_item,
                                        "NMR solvent",
                                    )
                            elif (
                                name == "acquisition nucleus"
                            ):  # Check if the variable is the acquisition nucleus
                                ontology_item = await self._terminology.searchForTerm(
                                    value,
                                    "chebi",
                                    "http://purl.obolibrary.org/obo/CHEBI_33250",  # has to be an atom
                                )  # Search for the term in the ChEBI ontology with the terminology service
                                if ontology_item is not None:
                                    fdo.addEntry(
                                        "21.T11969/1058eae15dac10260bb6",
                                        ontology_item,
                                        "Aquisition Nucleus",
                                    )
                            elif (
                                name == "irridation frequency"
                            ):  # Check if the variable is the irradiation frequency
                                fdo.addEntry(
                                    "21.T11969/1e6e84562ace3b58558d",
                                    value,
                                    "Nominal Proton Frequency",
                                )
                            elif name == "nuclear magnetic resonance pulse sequence":
                                fdo.addEntry(
                                    "21.T11969/3303cd9e3dda7afd6000",
                                    value,
                                    "Pulse Sequence Name",
                                )
                    except Exception as e:  # Log the error and raise it
                        logger.error(f"Error mapping variable {variable}: {str(e)}")
                        raise ValueError(f"Error mapping variable {variable}: {str(e)}")

            if (
                "isPartOf" in bioschema_dataset
                and bioschema_dataset["isPartOf"] is not None
            ):
                if isinstance(bioschema_dataset["isPartOf"], list):
                    for part in bioschema_dataset[
                        "isPartOf"
                    ]:  # Iterate over the parts of the dataset
                        if (
                            "name" in part
                        ):  # Add the name of the part to the PID record if available
                            new_name = f"{original_dataset["name"]}-{part["name"]}"
                            fdo.updateEntry("21.T11148/6ae999552a0d2dca14d6", new_name)
                        if "hasBioChemEntityPart" in part:
                            biochem_part = part["hasBioChemEntityPart"]
                            value = {}
                            if (
                                "molecularWeight" in biochem_part
                                and biochem_part["molecularWeight"] is not None
                            ):
                                value["21.T11969/6c4d3deac9a49b65886a"] = float(
                                    biochem_part["molecularWeight"]
                                )  # Add the molecular weight to the value of characterizedCompound if available
                            if (
                                "url" in biochem_part
                                and biochem_part["url"] is not None
                            ):
                                value["21.T11969/f9cb9b53273ce0da7739"] = biochem_part[
                                    "url"
                                ]  # Add the PubChem-URL to the value of characterizedCompound if available

                            if (
                                len(value) > 0
                            ):  # Add the value to the PID record if available
                                fdo.addEntry(
                                    "21.T11969/d15381199a44a16dc88d",
                                    value,
                                    "characterizedCompound",
                                )

                            if (
                                "chemicalFormula" in biochem_part
                            ):  # Check if the part has a chemical formula
                                formula = biochem_part["chemicalFormula"]
                                if (
                                    formula is not None
                                    and formula != ""
                                    and len(formula) > 1
                                ):  # Check for meaningful formula
                                    new_name = f"{original_dataset["name"]}-{formula}"  # Add the formula to the name of the part
                                    fdo.deleteEntry("21.T11969/6ae999552a0d2dca14d6")
                                    fdo.addEntry(
                                        "21.T11148/6ae999552a0d2dca14d6",
                                        new_name,
                                        "name",
                                    )

            return fdo
        except Exception as e:  # Log the error and raise it
            logger.error(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)
            raise ValueError(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)

    async def _mapSampleToPIDRecord(
        self,
        sample: dict,
        addRelationship: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord:
        """
        Maps a sample to a PID record.

        Args:
            sample (dict): The sample to map to a PID record. Contains the original and BioSchema data.
            addRelationship (function): The function to add relationships to a PIDRecord. For more information see AbstractRepository.

        Returns:
            PIDRecord: The PID record mapped from the sample.
        """
        # Extract the original and BioSchema data from the sample
        original_study = sample["original"]
        bioschema_study = sample["bioschema"]

        if (
            not original_study
            or original_study is None
            or not isinstance(original_study, dict)
            or not original_study["identifier"].startswith("NMRXIV:S")
        ):  # Check if the sample is valid
            raise ValueError(
                "The provided data doesnt contain an original study",
                sample,
                original_study,
            )
        elif (
            not bioschema_study
            or bioschema_study is None
            or not isinstance(bioschema_study, dict)
        ):  # Check if the BioSchema data is valid
            raise ValueError(
                "The provided data doesnt contain a bioschema study",
                sample,
                bioschema_study,
            )
        elif (
            "study_preview_urls" not in original_study
        ):  # Check if the sample has a study preview URL
            raise ValueError(
                "The provided original_study doesnt contain a study preview url and can therefore not be a study",
                sample,
                original_study,
            )
        elif (
            "@type" not in bioschema_study or not bioschema_study["@type"] == "Study"
        ):  # Check if the BioSchema data is a study
            raise ValueError(
                "The provided bioschema_study doesnt contain the correct @type value",
                sample,
                bioschema_study,
            )

        logger.info("mapping sample to FAIR-DO", sample)
        try:
            fdo = await self._mapGenericInfo2PIDRecord(
                sample
            )  # Get the generic information for the sample

            fdo.addEntry(
                "21.T11969/b736c3898dd1f6603e2c",
                "Study",
                "resourceType",
            )

            if (
                "public_url" in original_study
                and original_study["public_url"] is not None
            ):  # Add the public URL to the PID record as a landing page if available
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    original_study["public_url"],
                    "landingPageLocation",
                )
            elif (
                "url" in bioschema_study and bioschema_study["url"] is not None
            ):  # Add the URL to the PID record as a landing page if available
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    bioschema_study["url"],
                    "landingPageLocation",
                )

            if (
                "study_photo_urls" in original_study
                and original_study["study_photo_urls"] is not None
            ):  # Add the study photo URLs to the PID record as a preview if available
                for url in original_study["study_photo_urls"]:
                    fdo.addEntry(
                        "21.T11148/7fdada5846281ef5d461", url, "locationPreview"
                    )

            compoundEntries = []  # Initialize the list of compound entries
            if (
                "about" in bioschema_study
                and "hasBioChemEntityPart" in bioschema_study["about"]
                and bioschema_study["about"]["hasBioChemEntityPart"] is not None
            ):
                for part in bioschema_study["about"][
                    "hasBioChemEntityPart"
                ]:  # Iterate over the parts of the study
                    if not part or part is None:  # Check if the part is valid
                        logger.debug(
                            f"The provided part is empty. See {bioschema_study['@id']}"
                        )
                        continue

                    value: dict = {}

                    if (
                        "molecularWeight" in part
                        and part["molecularWeight"] is not None
                    ):  # Add the molecular weight to the value of characterizedCompound if available
                        value["21.T11969/6c4d3deac9a49b65886a"] = float(
                            part["molecularWeight"]
                        )
                    if (
                        "url" in part and part["url"] is not None
                    ):  # Add the PubChem-URL to the value of characterizedCompound if available
                        value["21.T11969/f9cb9b53273ce0da7739"] = part["url"]

                    if len(value) > 0:  # Add the value to the PID record if available
                        compoundEntries.append(
                            PIDRecordEntry(
                                "21.T11969/d15381199a44a16dc88d",
                                value,
                                "characterizedCompound",
                            )
                        )
                    else:
                        logger.warning(
                            f"The provided part does not contain a molecularWeight or url: {part}"
                        )

                    # mol = part["molecularWeight"]
                    # # formula = part[
                    # #     "molecularFormula"
                    # # ]  # TODO: use this in the name or topic
                    # # inchi = part["inChI"]
                    # pubchem = part["url"]

            elif (
                "molecules" in original_study
                and original_study["molecules"] is not None
            ):  # Add the molecules to the PID record if available and no BioChemEntityParts are available
                for molecule in original_study[
                    "molecules"
                ]:  # Iterate over the molecules
                    mol = molecule["molecular_weight"]
                    # formula = molecule[
                    #     "molecular_formula"
                    # ]  # TODO: use this in the name or topic
                    # inchi = molecule["standard_inchi"]
                    compoundEntries.append(
                        PIDRecordEntry(
                            "21.T11969/d15381199a44a16dc88d",
                            {  # characterisedCompound
                                "21.T11969/6c4d3deac9a49b65886a": mol,  # molecularWeight
                            },
                            "characterizedCompound",
                        )
                    )

            if (
                len(compoundEntries) > 0
            ):  # Add the compound entries to the PID record if available
                fdo.addListOfEntries(compoundEntries)

            if "hasPart" in bioschema_study and bioschema_study["hasPart"] is not None:
                for part in bioschema_study[
                    "hasPart"
                ]:  # Iterate over the parts of the study
                    if (
                        not part or part is None or "@id" not in part
                    ):  # Check if the part is valid
                        logger.error(
                            f"The provided part {part} in this study does not contain an @id"
                        )
                        continue

                    presumedDatasetID = encodeInBase64(
                        part["@id"].replace("https://doi.org/", "")
                    )  # Encode the dataset ID

                    datasetEntries = [
                        PIDRecordEntry(
                            "21.T11148/d0773859091aeb451528",
                            fdo.getPID(),
                            "hasMetadata",
                        ),
                    ]  # Initialize the list of dataset entries

                    # Add the preview image(s) to the dataset, if available
                    images = fdo.getEntry("21.T11148/7fdada5846281ef5d461")
                    if images is not None and isinstance(
                        images, list
                    ):  # Add the images to the dataset if available
                        for image in images:  # Iterate over the images
                            datasetEntries.append(
                                PIDRecordEntry(
                                    "21.T11148/7fdada5846281ef5d461",
                                    image,
                                    "locationPreview",
                                )
                            )
                    elif images is not None and isinstance(
                        images, str
                    ):  # Add the image to the dataset if available
                        datasetEntries.append(
                            PIDRecordEntry(
                                "21.T11148/7fdada5846281ef5d461",
                                images,
                                "locationPreview",
                            )
                        )

                    # TODO: Add formula to name or topic

                    if (
                        len(compoundEntries) > 0
                    ):  # Add the compound entries to the dataset if available
                        datasetEntries.extend(compoundEntries)

                    try:  # TODO: Abstract this

                        def add_metadata_entry(fdo_pid: str, pid: str) -> None:
                            """
                            Adds a metadata entry to the dataset.

                            Args:
                                fdo_pid (str): The PID of the FAIR-DO.
                                pid (str): The PID of the dataset.

                            Returns:
                                None
                            """
                            if pid is not None:
                                addRelationship(
                                    fdo_pid,
                                    [
                                        PIDRecordEntry(
                                            "21.T11148/4fe7cde52629b61e3b82",
                                            pid,
                                            "isMetadataFor",
                                        )
                                    ],
                                    None,
                                )

                        addRelationship(  # Add the dataset to the PID record
                            presumedDatasetID,  # The presumed PID of the dataset
                            datasetEntries,  # The predefined dataset entries from above
                            lambda pid: add_metadata_entry(
                                fdo.getPID(), pid
                            ),  # Callback function to add the metadata entry to the study
                        )
                    except Exception as e:  # Log the error and raise it
                        logger.error(
                            "Error adding dataset reference to study",
                            presumedDatasetID,
                            datasetEntries,
                            e,
                        )

            return fdo
        except Exception as e:  # Log the error and raise it
            logger.error(f"Error mapping sample to FAIR-DO: {str(e)}", sample)
            raise ValueError(f"Error mapping sample to FAIR-DO: {str(e)}", sample)

    async def _mapProjectToPIDRecord(
        self,
        project: dict,
        addEntries: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord:
        """
        Maps a project to a PID record.

        Args:
            project (dict): The project to map to a PID record. Contains the original and BioSchema data.
            addEntries (function): The function to add entries to a PIDRecord. For more information see AbstractRepository.

        Returns:
            PIDRecord: The PID record mapped from the project
        """
        # Extract the original and BioSchema data from the project
        original_project = project["original"]
        bioschema_project = project["bioschema"]

        if (
            not original_project
            or original_project is None
            or not isinstance(original_project, dict)
            or not original_project["identifier"].startswith("NMRXIV:P")
        ):  # Check if the project is valid
            raise ValueError(
                "Bad Request - The provided data is not a project", project
            )

        logger.info("mapping project to FAIR-DO", project)
        try:
            fdo = await self._mapGenericInfo2PIDRecord(
                project
            )  # Get the generic information for the project

            fdo.addEntry(
                "21.T11969/b736c3898dd1f6603e2c",
                "Project",
                "resourceType",
            )

            if (
                "public_url" in original_project
                and original_project["public_url"] is not None
            ):  # Add the public URL to the PID record as a landing page if available
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    original_project["public_url"],
                    "landingPageLocation",
                )
            elif (
                "url" in bioschema_project and bioschema_project["url"] is not None
            ):  # Add the URL to the PID record as a landing page if available
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    bioschema_project["url"],
                    "landingPageLocation",
                )

            if (
                "photo_url" in original_project
                and original_project["photo_url"] is not None
            ):  # Add the photo URL to the PID record as a preview if available
                fdo.addEntry(
                    "21.T11148/7fdada5846281ef5d461",
                    original_project["photo_url"],
                    "locationPreview",
                )

            if (
                "hasPart" in bioschema_project
                and bioschema_project["hasPart"] is not None
            ):
                for study in bioschema_project[
                    "hasPart"
                ]:  # Iterate over the studies of the project (if available)
                    if "@id" not in study:  # Check if the study has an ID
                        raise ValueError(
                            "The provided study in this project does not contain an @id",
                            project,
                        )

                    presumedStudyID = encodeInBase64(
                        study["@id"].replace("https://doi.org/", "")
                    )  # Encode the study ID

                    studyEntries = [
                        PIDRecordEntry(
                            "21.T11148/d0773859091aeb451528",
                            fdo.getPID(),
                            "hasMetadata",
                        ),
                    ]

                    try:

                        def add_metadata_entry(fdo_pid: str, pid: str):
                            """
                            Adds a metadata entry to the project.

                            Args:
                                fdo_pid (str): The PID of the FAIR-DO.
                                pid (str): The PID of the study.

                            Returns:
                                None
                            """
                            if pid is not None:
                                addEntries(
                                    fdo_pid,
                                    [
                                        PIDRecordEntry(
                                            "21.T11148/4fe7cde52629b61e3b82",
                                            pid,
                                            "isMetadataFor",
                                        )
                                    ],
                                    None,
                                )

                        addEntries(  # Add the study to the PID record
                            presumedStudyID,  # The presumed PID of the study
                            studyEntries,  # The predefined study entries from above
                            lambda pid: add_metadata_entry(
                                fdo.getPID(), pid
                            ),  # Callback function to add the metadata entry to the project
                        )
                    except Exception as e:  # Log the error and raise it
                        logger.error(
                            "Error adding study reference to project",
                            presumedStudyID,
                            studyEntries,
                            e,
                        )
            return fdo
        except Exception as e:  # Log the error and raise it
            logger.error(f"Error mapping project to FAIR-DO: {str(e)}", project)
            raise ValueError(f"Error mapping project to FAIR-DO: {str(e)}", project)

    def _removeDescription(self, resource: Any):
        """
        Removes the description from the specified resource. This is done for better readability and to reduce the size of the JSON-LD. The description field is not machine-readable and is therefore not needed.

        Args:
            resource (Any): The resource to remove the description from. If it is not a dictionary, the resource is returned as is.

        Returns:
            dict: The resource without the description.
            Any: The resource as is if it is not a dictionary.
        """

        if (
            not resource or resource is None or not isinstance(resource, dict)
        ):  # If the resource is not a dictionary, return it as is
            return resource

        if "description" in resource:  # If the resource has a description, remove it
            resource["description"] = None
        if "sdf" in resource:  # If the resource has an sdf, remove it
            resource["sdf"] = None

        def removeRecursively(key: str):
            """
            Removes the description from the specified key in the resource. This is done recursively for all parts of the resource.
            """
            if (
                key not in resource
            ):  # if the key is not in the resource, stop the function
                return
            parts = []
            if isinstance(resource[key], list):
                for part in resource[
                    key
                ]:  # if the key is a list, iterate over the list
                    parts.append(
                        self._removeDescription(part)
                    )  # remove the description from each part
            else:
                parts.append(
                    self._removeDescription(resource[key])
                )  # if the key is not a list, remove the description from the key
            resource[key] = parts  # set the key to the list of parts

        removeRecursively("hasPart")
        removeRecursively("isPartOf")
        removeRecursively("samples")
        removeRecursively("studies")

        return resource

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
            "21.T11148/7fdada5846281ef5d461",
            "https://avatars.githubusercontent.com/u/65726315",  # TODO: get the correct location preview
            "locationPreview",
        )

        fdo.addEntry(
            "21.T11148/aafd5fb4c7222e2d950a",
            datetime.now().isoformat(),
            "dateCreated",
        )

        fdo.addEntry(
            "21.T11148/6ae999552a0d2dca14d6",
            "NMRXiv",
            "name",
        )

        fdo.addEntry("21.T11969/b736c3898dd1f6603e2c", "Repository", "resourceType")

        return fdo
