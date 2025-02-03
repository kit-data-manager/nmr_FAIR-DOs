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
    _baseURL: str

    def __init__(
        self, baseURL: str, terminology: Terminology, fetch_fresh: bool = True
    ) -> None:
        if baseURL is not None and isinstance(baseURL, str):
            self._baseURL = baseURL
        else:
            self._baseURL = "https://nmrxiv.org"

        if terminology is not None and isinstance(terminology, Terminology):
            self._terminology = terminology
        else:
            raise ValueError("Terminology must be an instance of Terminology")

        self._fetch_fresh = fetch_fresh if fetch_fresh is not None else True

    @property
    def repositoryID(self) -> str:
        return "NMRXiv_" + self._baseURL

    async def getAllAvailableResources(self) -> list[dict] | None:
        return await self.getResourcesForTimeFrame(datetime.min, datetime.max)

    async def getResourcesForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[dict]:
        result: list[dict] = []
        if self._fetch_fresh or not os.path.isfile("nmrxiv_resources.json"):
            result.extend(await self._getResourcesForCategory("datasets", start, end))
            result.extend(await self._getResourcesForCategory("samples", start, end))
            result.extend(await self._getResourcesForCategory("projects", start, end))

            with open("nmrxiv_resources.json", "w") as f:
                json.dump(result, f)
        else:
            with open("nmrxiv_resources.json", "r") as f:
                result = json.load(f)
                if result is None or not isinstance(result, list):
                    raise ValueError("Invalid resources file", result)
        return result

    async def extractPIDRecordFromResource(
        self,
        resource: dict,
        addEntries: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord | None:
        if not resource or resource is None or not isinstance(resource, dict):
            raise ValueError("Invalid resource.")
        elif "original" not in resource or "bioschema" not in resource:
            raise ValueError("Resource is missing original or bioschema data.")

        if "doi" not in resource["original"]:
            raise ValueError("Resource has no DOI.")

        first_letter_type_indicator = resource["original"]["identifier"].replace(
            "NMRXIV:", ""
        )[0]

        if first_letter_type_indicator == "D":
            return await self._mapDatasetToPIDRecord(resource)
        elif first_letter_type_indicator == "S":
            return await self._mapSampleToPIDRecord(resource, addEntries)
        elif first_letter_type_indicator == "P":
            return await self._mapProjectToPIDRecord(resource, addEntries)
        else:
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
        ):
            raise ValueError(
                "Start date and end date cannot be empty and must be a datetime."
            )
        if start > end:
            raise ValueError("Start date must be before end date.")
        if start > datetime.now():
            raise ValueError("Start date must be in the past.")

        if (
            not category
            or category == ""
            or category not in ["datasets", "samples", "projects"]
        ):
            raise ValueError(
                "Category cannot be empty and must be either 'datasets' or 'samples' ."
            )

        # Remove the timezone information from the datetime objects
        start = start.replace(tzinfo=None)
        end = end.replace(tzinfo=None)

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
            ):
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
                        objects.append(await self._getBioChemIntegratedDict(elem))
                    elif (
                        updated is not None and start <= updated <= end
                    ):  # Check if the update date is in the timerange (if available)
                        logger.debug(
                            f"Update date of the resource {elem['doi']} is in the timerange."
                        )
                        objects.append(await self._getBioChemIntegratedDict(elem))
                    else:
                        logger.debug(f"Resource {elem['doi']} is not in the timerange.")
                        continue
                except Exception as e:
                    logger.error(
                        f"Error fetching BioSchema for resource {elem['doi']}: {str(e)}",
                        elem,
                        e,
                    )

            next_url = response["links"]["next"]

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

        bioschema = await fetch_data(url)

        if not bioschema or bioschema is None or not isinstance(bioschema, dict):
            raise ValueError("Invalid BioSchema JSON.", bioschema, url)

        return {
            "original": self._removeDescription(elem),
            "bioschema": self._removeDescription(bioschema),
        }

    async def _mapGenericInfo2PIDRecord(self, resource) -> PIDRecord:
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
                f"Mapping generic info to PID Record: {original_resource["doi"]}"
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

            if "created_at" in original_resource:
                fdo.addEntry(
                    "21.T11148/aafd5fb4c7222e2d950a",
                    parseDateTime(original_resource["created_at"]).isoformat(),
                    "dateCreated",
                )

            if "updated_at" in original_resource:
                fdo.addEntry(
                    "21.T11148/397d831aa3a9d18eb52c",
                    parseDateTime(original_resource["updated_at"]).isoformat(),
                    "dateModified",
                )

            if "name" in original_resource:
                fdo.addEntry(
                    "21.T11148/6ae999552a0d2dca14d6", original_resource["name"], "name"
                )

            if "identifier" in original_resource:
                fdo.addEntry(
                    "21.T11148/f3f0cbaa39fa9966b279",
                    original_resource["identifier"].replace("NMRXIV:", ""),
                    "identifier",
                )
            fdo.addEntry(
                "21.T11148/f3f0cbaa39fa9966b279",
                original_resource["doi"].replace("https://doi.org/", ""),
                "identifier",
            )

            if (
                "license" in original_resource
                and "spdx_id" in original_resource["license"]
            ):
                fdo.addEntry(
                    "21.T11148/2f314c8fe5fb6a0063a8",
                    await parseSPDXLicenseURL(original_resource["license"]["spdx_id"]),
                    "license",
                )
            elif "license" in bioschema_resource:
                fdo.addEntry(
                    "21.T11148/2f314c8fe5fb6a0063a8",
                    await parseSPDXLicenseURL(bioschema_resource["license"]),
                    "license",
                )

            if "authors" in original_resource and isinstance(
                original_resource["authors"], list
            ):
                for author in original_resource["authors"]:
                    if "orcid_id" in author:
                        fdo.addEntry(
                            "21.T11148/1a73af9e7ae00182733b",
                            "https://orcid.org/" + author["orcid_id"],
                            "contact",
                        )
                    elif "email" in author:
                        fdo.addEntry(
                            "21.T11148/e117a4a29bfd07438c1e",
                            author["email"],
                            "emailContact",
                        )
            elif "owner" in original_resource and "email" in original_resource["owner"]:
                fdo.addEntry(
                    "21.T11148/e117a4a29bfd07438c1e",
                    original_resource["owner"]["email"],
                    "emailContact",
                )
            elif "users" in original_resource:
                for user in original_resource["users"]:
                    if "email" in user:
                        fdo.addEntry(
                            "21.T11148/e117a4a29bfd07438c1e",
                            user["email"],
                            "emailContact",
                        )
                        # fdo.addEntry( # TODO: get an ORCiD not just an email
                        #     "21.T11148/1a73af9e7ae00182733b",
                        #     resource["owner"]["email"],
                        #     "contact",
                        # )

            logger.debug(f"Mapped generic info to FAIR-DO: {fdo.getPID()}")
            return fdo
        except Exception as e:
            logger.error(f"Error mapping generic info to FAIR-DO: {str(e)}", resource)
            raise ValueError(
                f"Error mapping generic info to FAIR-DO: {str(e)}", resource
            )

    async def _mapDatasetToPIDRecord(self, dataset: dict) -> PIDRecord:
        original_dataset = dataset["original"]
        bioschema_dataset = dataset["bioschema"]

        if (
            not original_dataset
            or original_dataset is None
            or not isinstance(original_dataset, dict)
            or not original_dataset["identifier"].startswith("NMRXIV:D")
            or "@type" not in bioschema_dataset
            or bioschema_dataset["@type"] != "Dataset"
        ):
            raise ValueError(
                "Bad Request - The provided data is not a dataset", dataset
            )

        try:
            logger.info(f"mapping dataset to FAIR-DO: {bioschema_dataset["@id"]}")
            fdo = await self._mapGenericInfo2PIDRecord(dataset)

            fdo.addEntry(
                "21.T11969/b736c3898dd1f6603e2c",
                "Dataset",
                "resourceType",  # TODO: use PID to refer to the resourceType
            )

            if "doi" in original_dataset:
                url = f"https://dx.doi.org/{original_dataset["doi"].replace("https://doi.org/", "")}"
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    url,
                    "digitalObjectLocation",
                )

            if "measurementTechnique" in bioschema_dataset:
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

            if "public_url" in original_dataset:
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    original_dataset["public_url"],
                    "landingPageLocation",
                )
            elif "url" in bioschema_dataset:
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    bioschema_dataset["url"],
                    "landingPageLocation",
                )

            if "dataset_photo_url" in original_dataset:
                fdo.addEntry(
                    "21.T11148/7fdada5846281ef5d461",
                    original_dataset["dataset_photo_url"],
                    "locationPreview",
                )

            if "variableMeasured" in bioschema_dataset:
                for variable in bioschema_dataset["variableMeasured"]:
                    try:
                        if "name" not in variable or "value" not in variable:
                            logger.warning(
                                f"Skipping variable {variable} because it has no name or value"
                            )
                            continue

                        name = variable["name"]
                        values = variable["value"]

                        if values is None:
                            logger.warning(
                                f"Skipping variable {name} because it has no value"
                            )
                            continue
                        elif not isinstance(values, list):
                            values = [values]

                        for value in values:
                            if not isinstance(value, str):
                                logger.warning(
                                    f"Skipping variable {name} because value {value} is not a string"
                                )
                                continue
                            logger.debug(
                                f"Evaluating variable {name} with value {value}"
                            )
                            if name == "NMR solvent":
                                ontology_item = await self._terminology.searchForTerm(
                                    value,
                                    "chebi",
                                    "http://purl.obolibrary.org/obo/CHEBI_197449",
                                )
                                fdo.addEntry(
                                    "21.T11969/92b4c6b461709b5b36f5",
                                    ontology_item
                                    if ontology_item is not None
                                    else value,
                                    "NMR solvent",
                                )
                            elif name == "acquisition nucleus":
                                ontology_item = await self._terminology.searchForTerm(
                                    value,
                                    "chebi",
                                    "http://purl.obolibrary.org/obo/CHEBI_33250",
                                )
                                fdo.addEntry(
                                    "21.T11969/1058eae15dac10260bb6",
                                    ontology_item
                                    if ontology_item is not None
                                    else value,
                                    "Aquisition Nucleus",
                                )
                            elif name == "irridation frequency":
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
                    except Exception as e:
                        logger.error(f"Error mapping variable {variable}: {str(e)}")
                        raise ValueError(f"Error mapping variable {variable}: {str(e)}")

            if "isPartOf" in bioschema_dataset:
                if isinstance(bioschema_dataset["isPartOf"], list):
                    for part in bioschema_dataset["isPartOf"]:
                        if "name" in part:
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
                                )
                            if (
                                "url" in biochem_part
                                and biochem_part["url"] is not None
                            ):
                                value["21.T11969/f9cb9b53273ce0da7739"] = biochem_part[
                                    "url"
                                ]

                            if len(value) > 0:
                                fdo.addEntry(
                                    "21.T11969/d15381199a44a16dc88d",
                                    value,
                                    "characterizedCompound",
                                )

                            if "chemicalFormula" in biochem_part:
                                formula = biochem_part["chemicalFormula"]
                                if (
                                    formula is not None
                                    and formula != ""
                                    and len(formula) > 1
                                ):  # Check for meaningful formula
                                    new_name = f"{original_dataset["name"]}-{formula}"
                                    fdo.deleteEntry("21.T11969/6ae999552a0d2dca14d6")
                                    fdo.addEntry(
                                        "21.T11148/6ae999552a0d2dca14d6",
                                        new_name,
                                        "name",
                                    )

            # fdo.addEntry(
            #     "21.T11148/82e2503c49209e987740",
            #     "TODO",  # TODO: get the correct checksum
            #     "checksum",
            # )

            return fdo
        except Exception as e:
            logger.error(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)
            raise ValueError(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)

    async def _mapSampleToPIDRecord(
        self,
        sample: dict,
        addEntries: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord:
        original_study = sample["original"]
        bioschema_study = sample["bioschema"]

        if (
            not original_study
            or original_study is None
            or not isinstance(original_study, dict)
            or not original_study["identifier"].startswith("NMRXIV:S")
        ):
            raise ValueError(
                "The provided data doesnt contain an original study",
                sample,
                original_study,
            )
        elif (
            not bioschema_study
            or bioschema_study is None
            or not isinstance(bioschema_study, dict)
        ):
            raise ValueError(
                "The provided data doesnt contain a bioschema study",
                sample,
                bioschema_study,
            )
        elif "study_preview_urls" not in original_study:
            raise ValueError(
                "The provided original_study doesnt contain a study preview url and can therefore not be a study",
                sample,
                original_study,
            )
        elif "@type" not in bioschema_study or not bioschema_study["@type"] == "Study":
            raise ValueError(
                "The provided bioschema_study doesnt contain the correct @type value",
                sample,
                bioschema_study,
            )

        logger.info("mapping sample to FAIR-DO", sample)
        try:
            fdo = await self._mapGenericInfo2PIDRecord(sample)

            fdo.addEntry(
                "21.T11969/b736c3898dd1f6603e2c",
                "Study",
                "resourceType",  # TODO: use PID to refer to the resourceType
            )

            if "download_url" in original_study:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_study["download_url"],
                    "digitalObjectLocation",
                )
            else:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    f"https://dx.doi.org/{original_study["doi"].replace("https://doi.org/", "")}",
                    "digitalObjectLocation",
                )

            if "public_url" in original_study:
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    original_study["public_url"],
                    "landingPageLocation",
                )
            elif "url" in bioschema_study:
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    bioschema_study["url"],
                    "landingPageLocation",
                )

            if "study_photo_urls" in original_study:
                for url in original_study["study_photo_urls"]:
                    fdo.addEntry(
                        "21.T11148/7fdada5846281ef5d461", url, "locationPreview"
                    )

            # fdo.addEntry(
            #     "21.T11148/82e2503c49209e987740",
            #     "TODO",  # TODO: get the correct checksum
            #     "checksum",
            # )

            compoundEntries = []
            if (
                "about" in bioschema_study
                and "hasBioChemEntityPart" in bioschema_study["about"]
            ):
                for part in bioschema_study["about"]["hasBioChemEntityPart"]:
                    if not part or part is None:
                        logger.debug(
                            f"The provided part is empty. See {bioschema_study['@id']}"
                        )
                        continue

                    value: dict = {}

                    if (
                        "molecularWeight" in part
                        and part["molecularWeight"] is not None
                    ):
                        value["21.T11969/6c4d3deac9a49b65886a"] = float(
                            part["molecularWeight"]
                        )
                    if "url" in part and part["url"] is not None:
                        value["21.T11969/f9cb9b53273ce0da7739"] = part["url"]

                    if len(value) > 0:
                        compoundEntries.append(
                            PIDRecordEntry(
                                "21.T11969/d15381199a44a16dc88d",
                                value,
                                "characterizedCompound",
                            )
                        )
                    else:
                        logger.warn(
                            f"The provided part does not contain a molecularWeight or url: {part}"
                        )

                    # mol = part["molecularWeight"]
                    # # formula = part[
                    # #     "molecularFormula"
                    # # ]  # TODO: use this in the name or topic
                    # # inchi = part["inChI"]
                    # pubchem = part["url"]

            elif "molecules" in original_study:
                for molecule in original_study["molecules"]:
                    mol = molecule["molecular_weight"]
                    # formula = molecule[
                    #     "molecular_formula"
                    # ]  # TODO: use this in the name or topic
                    # inchi = molecule["standard_inchi"]
                    compoundEntries.append(
                        PIDRecordEntry(
                            "21.T11969/d15381199a44a16dc88d",
                            {
                                "21.T11969/6c4d3deac9a49b65886a": mol,
                            },
                            "characterizedCompound",
                        )
                    )

            if len(compoundEntries) > 0:
                fdo.addListOfEntries(compoundEntries)

            if "hasPart" in bioschema_study:
                for part in bioschema_study["hasPart"]:
                    if not part or part is None or "@id" not in part:
                        logger.error(
                            f"The provided part {part} in this study does not contain an @id"
                        )
                        continue

                    presumedDatasetID = encodeInBase64(
                        part["@id"].replace("https://doi.org/", "")
                    )

                    datasetEntries = [
                        PIDRecordEntry(
                            "21.T11148/d0773859091aeb451528",
                            fdo.getPID(),
                            "hasMetadata",
                        ),
                    ]

                    # Add the preview image(s) to the dataset, if available
                    images = fdo.getEntry("21.T11148/7fdada5846281ef5d461")
                    if images is not None and isinstance(images, list):
                        for image in images:
                            datasetEntries.append(
                                PIDRecordEntry(
                                    "21.T11148/7fdada5846281ef5d461",
                                    image,
                                    "locationPreview",
                                )
                            )
                    elif images is not None and isinstance(images, str):
                        datasetEntries.append(
                            PIDRecordEntry(
                                "21.T11148/7fdada5846281ef5d461",
                                images,
                                "locationPreview",
                            )
                        )

                    # TODO: Add formula to name or topic

                    if len(compoundEntries) > 0:
                        datasetEntries.extend(compoundEntries)

                    try:  # TODO: Abstract this

                        def add_metadata_entry(fdo_pid: str, pid: str) -> None:
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

                        addEntries(
                            presumedDatasetID,
                            datasetEntries,
                            lambda pid: add_metadata_entry(fdo.getPID(), pid),
                        )
                    except Exception as e:
                        logger.error(
                            "Error adding dataset reference to study",
                            presumedDatasetID,
                            datasetEntries,
                            e,
                        )

            return fdo
        except Exception as e:
            logger.error(f"Error mapping sample to FAIR-DO: {str(e)}", sample)
            raise ValueError(f"Error mapping sample to FAIR-DO: {str(e)}", sample)

    async def _mapProjectToPIDRecord(
        self,
        project: dict,
        addEntries: Callable[
            [str, list[PIDRecordEntry], Callable[[str], None] | None], str
        ],
    ) -> PIDRecord:
        original_project = project["original"]
        bioschema_project = project["bioschema"]

        if (
            not original_project
            or original_project is None
            or not isinstance(original_project, dict)
            or not original_project["identifier"].startswith("NMRXIV:P")
        ):
            raise ValueError(
                "Bad Request - The provided data is not a project", project
            )

        logger.info("mapping project to FAIR-DO", project)
        try:
            fdo = await self._mapGenericInfo2PIDRecord(project)

            fdo.addEntry(
                "21.T11969/b736c3898dd1f6603e2c",
                "Project",
                "resourceType",  # TODO: use PID to refer to the resourceType
            )

            if "download_url" in original_project:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_project["download_url"],
                    "digitalObjectLocation",
                )
            else:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    f"https://dx.doi.org/{original_project["doi"].replace("https://doi.org/", "")}",
                    "digitalObjectLocation",
                )

            if "public_url" in original_project:
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    original_project["public_url"],
                    "landingPageLocation",
                )
            elif "url" in bioschema_project:
                fdo.addEntry(
                    "21.T11969/8710d753ad10f371189b",
                    bioschema_project["url"],
                    "landingPageLocation",
                )

            if "photo_url" in original_project:
                fdo.addEntry(
                    "21.T11148/7fdada5846281ef5d461",
                    original_project["photo_url"],
                    "locationPreview",
                )

            if "hasPart" in bioschema_project:
                for study in bioschema_project["hasPart"]:
                    if "@id" not in study:
                        raise ValueError(
                            "The provided study in this project does not contain an @id",
                            project,
                        )

                    presumedStudyID = encodeInBase64(
                        study["@id"].replace("https://doi.org/", "")
                    )

                    studyEntries = [
                        PIDRecordEntry(
                            "21.T11148/d0773859091aeb451528",
                            fdo.getPID(),
                            "hasMetadata",
                        ),
                    ]

                    try:

                        def add_metadata_entry(fdo_pid: str, pid: str):
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

                        addEntries(
                            presumedStudyID,
                            studyEntries,
                            lambda pid: add_metadata_entry(fdo.getPID(), pid),
                        )
                    except Exception as e:
                        logger.error(
                            "Error adding study reference to project",
                            presumedStudyID,
                            studyEntries,
                            e,
                        )
            return fdo
        except Exception as e:
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

        if not resource or resource is None or not isinstance(resource, dict):
            return resource

        if "description" in resource:
            resource["description"] = None
        if "sdf" in resource:
            resource["sdf"] = None

        def removeRecursively(key: str):
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
