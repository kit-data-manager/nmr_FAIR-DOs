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
from nmr_FAIR_DOs.utils import encodeInBase64, fetch_data

logger = logging.getLogger(__name__)


class NMRXiv_Repository(AbstractRepository):
    _baseURL: str

    def __init__(self, baseURL: str) -> None:
        if baseURL is not None and isinstance(baseURL, str):
            self._baseURL = baseURL
        else:
            self._baseURL = "https://nmrxiv.org"

    @property
    def repositoryID(self) -> str:
        return "NMRXiv_" + self._baseURL

    async def getAllAvailableResources(self) -> list[dict] | None:
        return await self.listResourcesForTimeFrame(datetime.min, datetime.max)

    async def getResourcesForTimeFrame(
        self, start: datetime, end: datetime
    ) -> list[dict]:
        result: list[dict] = []
        result.extend(await self._getResourcesForCategory("datasets", start, end))
        result.extend(await self._getResourcesForCategory("samples", start, end))
        return result

    async def extractPIDRecordFromResource(
        self, resource: dict, addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> PIDRecord | None:
        if not resource or resource is None or not isinstance(resource, dict):
            raise ValueError("Invalid resource.")

        if "doi" not in resource:
            raise ValueError("Resource has no DOI.")

        first_letter_type_indicator = resource["original"]["identifier"].replace(
            "NMRXIV:", ""
        )[0]

        if first_letter_type_indicator == "D":
            return self._mapDatasetToPIDRecord(resource)
        elif first_letter_type_indicator == "S":
            return self._mapSampleToPIDRecord(resource, addEntries)
        elif first_letter_type_indicator == "P":
            return self._mapProjectToPIDRecord(resource, addEntries)
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

        if not category or category == "" or category not in ["datasets", "samples"]:
            raise ValueError(
                "Category cannot be empty and must be either 'datasets' or 'samples' ."
            )

        url_template = Template("$repositoryURL/api/v1/list/$category?page=$page")
        page = 1
        complete = False
        objects: list[dict] = []

        while not complete:  # Loop until all entries are fetched
            # Create the URL
            url = url_template.safe_substitute(
                repositoryURL=self._baseURL, category=category
            )
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
                    datetime.fromisoformat(elem["created"])
                    if "created" in elem
                    else None
                )  # Extract the creation date
                updated = (
                    datetime.fromisoformat(elem["updated"])
                    if "updated" in elem
                    else None
                )  # Extract the update date, if available

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
                    objects.append(elem)
                elif (
                    updated is not None and start <= updated <= end
                ):  # Check if the update date is in the timerange (if available)
                    logger.debug(
                        f"Update date of the resource {elem['doi']} is in the timerange."
                    )
                    objects.append(elem)
                else:
                    logger.debug(f"Resource {elem['doi']} is not in the timerange.")
                    continue

            next_url = response["links"]["next"]

            if (
                not next_url or next_url == "" or next_url == "null"
            ):  # Check if there are more pages by looking at the "next" link
                complete = True  # If there are no more pages, stop the loop
            else:
                page += 1  # If there are more pages, increment the page number

        # Log the number of URLs found and return them
        logger.info(f"found {len(objects)} urls\n\n")
        return objects

    async def _getBioChemIntegratedDict(self, elem: dict) -> dict:
        """
        Fetches the JSON-LD representation of the BioSchema for the specified ID.

        Args:
            id (str): The ID of the BioSchema.

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

        template = Template("$repositoryURL/api/v1/bioschemas/$id")
        url = template.safe_substitute(repositoryURL=self._baseURL, id=identifier)
        logger.debug("Getting BioSchema JSON for " + url)

        bioschema = await fetch_data(url)

        if not bioschema or bioschema is None or not isinstance(bioschema, dict):
            raise ValueError("Invalid BioSchema JSON.", bioschema, url)

        return {"original": elem, "bioschema": bioschema}

    def _mapGenericInfo2PIDRecord(self, resource) -> PIDRecord:
        """
        Maps generic information to a PID record.

        Args:
            resource (dict): The JSON response from the NMRXiv API.

        Returns:
            PIDRecord: The PID record mapped from the generic information
        """
        logger.debug("Mapping generic info to PID Record", resource["@id"])

        original_resource = resource["original"]
        bioschema_resource = resource["bioschema"]
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

        fdo.addEntry(
            "21.T11148/a753134738da82809fc1",
            self._baseURL,  # TODO: Refer to FAIR-DO of the repository (via Handle PID)
            "hadPrimarySource",
        )

        # fdo.addEntry(
        #     "21.T11148/b8457812905b83046284",
        #     fdo.getPID(),
        #     "digitalObjectLocation"
        # )
        #
        # fdo.addEntry(
        #     "21.T11148/8710d753ad10f371189b",
        #     fdo.getPID(),
        #     "landingPageLocation",
        # )

        if "created_at" in original_resource:
            fdo.addEntry(
                "21.T11148/aafd5fb4c7222e2d950a",
                original_resource["created_at"],
                "dateCreated",
            )

        if "modified_at" in original_resource:
            fdo.addEntry(
                "21.T11148/397d831aa3a9d18eb52c",
                original_resource["updated_at"],  # TODO: Parse datetime
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

        if "license" in original_resource:
            fdo.addEntry(
                "21.T11148/2f314c8fe5fb6a0063a8",
                "https://spdx.org/licenses/" + original_resource["license"]["spdx_id"],
                "license",
            )
        elif "license" in bioschema_resource:
            fdo.addEntry(
                "21.T11148/2f314c8fe5fb6a0063a8",
                bioschema_resource["license"],
                "license",
            )

        if "authors" in original_resource and isinstance(
            original_resource["authors"], list
        ):
            for author in original_resource["authors"]:
                if "@id" in author:
                    fdo.addEntry(
                        "21.T11148/1a73af9e7ae00182733b",
                        "https://orcid.org/" + author["orcid_id"],
                        "contact",
                    )
        elif "owner" in original_resource:
            fdo.addEntry(
                "21.T11148/e117a4a29bfd07438c1e",
                original_resource["owner"]["email"],
                "emailContact",
            )
        elif "users" in original_resource:
            for user in original_resource["users"]:
                if "email" in user:
                    fdo.addEntry(
                        "21.T11148/e117a4a29bfd07438c1e", user["email"], "emailContact"
                    )
                    # fdo.addEntry( # TODO: get an ORCiD not just an email
                    #     "21.T11148/1a73af9e7ae00182733b",
                    #     resource["owner"]["email"],
                    #     "contact",
                    # )

        logger.debug("Mapped generic info to FAIR-DO", fdo)
        return fdo

    def _mapDatasetToPIDRecord(self, dataset: dict) -> PIDRecord:
        original_dataset = dataset["original"]
        bioschema_dataset = dataset["bioschema"]

        if (
            not original_dataset
            or original_dataset is None
            or not isinstance(original_dataset, dict)
            or "study" not in original_dataset
            or "study_preview_urls" in original_dataset
            or bioschema_dataset["@type"] != "Dataset"
        ):
            raise ValueError(
                "Bad Request - The provided data is not a dataset", dataset
            )

        logger.info("mapping dataset to FAIR-DO", dataset["@id"])
        try:
            fdo = self._mapGenericInfo2PIDRecord(dataset)

            fdo.addEntry(
                "21.T11969/a00985b98dac27bd32f8",
                "Dataset",
                "resourceType",  # TODO: use PID to refer to the resourceType
            )

            fdo.addEntry(
                "21.T11148/b8457812905b83046284",
                original_dataset["doi"],
                "digitalObjectLocation",
            )

            if "measurementTechnique" in bioschema_dataset:
                fdo.addEntry(
                    "21.T11969/7a19f6d5c8e63dd6bfcb",
                    bioschema_dataset["measurementTechnique"]["url"],
                    "NMR method",
                )

            if "public_url" in original_dataset:
                fdo.addEntry(
                    "21.T11148/8710d753ad10f371189b",
                    original_dataset["public_url"],
                    "landingPageLocation",
                )
            elif "url" in bioschema_dataset:
                fdo.addEntry(
                    "21.T11148/8710d753ad10f371189b",
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
                    if variable["name"] == "NMR solvent":
                        fdo.addEntry(
                            "21.T11969/92b4c6b461709b5b36f5",
                            variable["value"],
                            "NMR solvent",
                        )
                    elif variable["name"] == "acquisition nucleus":
                        fdo.addEntry(
                            "21.T11969/1058eae15dac10260bb6",
                            variable["value"][0],
                            "Aquisition Nucleus",
                        )
                    elif variable["name"] == "irridation frequency":
                        fdo.addEntry(
                            "21.T11969/1e6e84562ace3b58558d",
                            variable["value"][0],
                            "Nominal Proton Frequency",
                        )
                    elif (
                        variable["name"] == "nuclear magnetic resonance pulse sequence"
                    ):
                        fdo.addEntry(
                            "21.T11969/3303cd9e3dda7afd6000",
                            variable["value"],
                            "Pulse Sequence Name",
                        )

            # fdo.addEntry(
            #     "21.T11969/7a19f6d5c8e63dd6bfcb",
            #     original_dataset["measurementTechnique"]["@id"],
            #     "NMR method",
            # )
            #
            # fdo.addEntry(
            #     "21.T11148/2f314c8fe5fb6a0063a8", original_dataset["license"], "license"
            # )

            # entries.append({
            #     "key": "21.T11969/d15381199a44a16dc88d",
            #     "name": "characterizedCompound",
            #     "value": dataset["isPartOf"][0]["about"][0]["hasBioChemEntityPart"]["molecularWeight"]["value"]
            # }

            fdo.addEntry(
                "21.T11148/82e2503c49209e987740",
                "TODO",  # TODO: get the correct checksum
                "checksum",
            )

            return fdo
        except Exception as e:
            logger.error(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)
            raise ValueError(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)

    def _mapSampleToPIDRecord(
        self, sample: dict, addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> PIDRecord:
        original_study = sample["original"]
        bioschema_study = sample["bioschema"]

        if (
            not original_study
            or original_study is None
            or not isinstance(original_study, dict)
            or "sample" not in original_study
            or "study_preview_urls" in original_study
            or bioschema_study["@type"] != "Study"
        ):
            raise ValueError("Bad Request - The provided data is not a sample", sample)

        logger.info("mapping sample to FAIR-DO", sample)
        try:
            fdo = self._mapGenericInfo2PIDRecord(sample)

            fdo.addEntry(
                "21.T11969/a00985b98dac27bd32f8",
                "Study",
                "resourceType",  # TODO: use PID to refer to the resourceType
            )

            # if "measurementTechnique" in bioschema_study:
            #     fdo.addEntry(
            #         "21.T11969/7a19f6d5c8e63dd6bfcb",
            #         bioschema_study["measurementTechnique"]["url"],
            #         "NMR method",
            #     )

            if "download_url" in original_study:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_study["download_url"],
                    "digitalObjectLocation",
                )
            else:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_study["doi"],
                    "digitalObjectLocation",
                )

            if "public_url" in original_study:
                fdo.addEntry(
                    "21.T11148/8710d753ad10f371189b",
                    original_study["public_url"],
                    "landingPageLocation",
                )
            elif "url" in bioschema_study:
                fdo.addEntry(
                    "21.T11148/8710d753ad10f371189b",
                    bioschema_study["url"],
                    "landingPageLocation",
                )

            if "study_photo_urls" in original_study:
                for url in original_study["study_photo_urls"]:
                    fdo.addEntry(
                        "21.T11148/7fdada5846281ef5d461", url, "locationPreview"
                    )

            # fdo.addEntry(
            #     "21.T11969/7a19f6d5c8e63dd6bfcb",
            #     original_dataset["measurementTechnique"]["@id"],
            #     "NMR method",
            # )
            #
            # fdo.addEntry(
            #     "21.T11148/2f314c8fe5fb6a0063a8", original_dataset["license"], "license"
            # )

            # entries.append({
            #     "key": "21.T11969/d15381199a44a16dc88d",
            #     "name": "characterizedCompound",
            #     "value": dataset["isPartOf"][0]["about"][0]["hasBioChemEntityPart"]["molecularWeight"]["value"]
            # }

            fdo.addEntry(
                "21.T11148/82e2503c49209e987740",
                "TODO",  # TODO: get the correct checksum
                "checksum",
            )

            compoundEntries = []
            if (
                "about" in bioschema_study
                and "hasBioChemEntityPart" in bioschema_study["about"]
            ):
                for part in bioschema_study["about"]["hasBioChemEntityPart"]:
                    mol = part["molecularWeight"]
                    # formula = part[
                    #     "molecularFormula"
                    # ]  # TODO: use this in the name or topic
                    # inchi = part["inChI"]
                    pubchem = part["url"]

                    compoundEntries.append(
                        PIDRecordEntry(
                            "21.T11969/d15381199a44a16dc88d",
                            {
                                "21.T11969/6c4d3deac9a49b65886a": mol,
                                "21.T11969/f9cb9b53273ce0da7739": pubchem,
                            },
                            "characterizedCompound",
                        )
                    )
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
                            mol,
                            "characterizedCompound",
                        )
                    )

            if len(compoundEntries) > 0:
                fdo.addListOfEntries(compoundEntries)

            if "hasPart" in bioschema_study:
                for dataset in bioschema_study["about"]:
                    presumedDatasetID = encodeInBase64(
                        dataset["@id"].replace("https://doi.org/", ""), "utf-8"
                    )

                    datasetEntries = [
                        PIDRecordEntry(
                            "21.T11148/d0773859091aeb451528",
                            fdo.getPID(),
                            "hasMetadata",
                        ),
                    ]

                    # TODO: Add formula to name or topic

                    if len(compoundEntries) > 0:
                        datasetEntries.extend(compoundEntries)

                    try:
                        datasetPID = addEntries(presumedDatasetID, datasetEntries)
                        if datasetPID is not None:
                            fdo.addEntry(
                                "21.T11148/4fe7cde52629b61e3b82",
                                datasetPID,
                                "isMetadataFor",
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
            logger.error(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)
            raise ValueError(f"Error mapping dataset to FAIR-DO: {str(e)}", dataset)

    def _mapProjectToPIDRecord(
        self, project: dict, addEntries: Callable[[str, list[PIDRecordEntry]], str]
    ) -> PIDRecord:
        original_project = project["original"]
        bioschema_project = project["bioschema"]

        if (
            not original_project
            or original_project is None
            or not isinstance(original_project, dict)
            or "studies" not in original_project
            or not isinstance(original_project["studies"], list)
            or not (
                original_project["identifier"].replace("NMRXIV:", "").startsWith("P")
            )
        ):
            raise ValueError(
                "Bad Request - The provided data is not a project", project
            )

        logger.info("mapping project to FAIR-DO", project)
        try:
            fdo = self._mapGenericInfo2PIDRecord(project)

            fdo.addEntry(
                "21.T11969/a00985b98dac27bd32f8",
                "Project",
                "resourceType",  # TODO: use PID to refer to the resourceType
            )

            # if "measurementTechnique" in bioschema_project:
            #     fdo.addEntry(
            #         "21.T11969/7a19f6d5c8e63dd6bfcb",
            #         bioschema_project["measurementTechnique"]["url"],
            #         "NMR method",
            #     )

            if "download_url" in original_project:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_project["download_url"],
                    "digitalObjectLocation",
                )
            else:
                fdo.addEntry(
                    "21.T11148/b8457812905b83046284",
                    original_project["doi"],
                    "digitalObjectLocation",
                )

            if "public_url" in original_project:
                fdo.addEntry(
                    "21.T11148/8710d753ad10f371189b",
                    original_project["public_url"],
                    "landingPageLocation",
                )
            elif "url" in bioschema_project:
                fdo.addEntry(
                    "21.T11148/8710d753ad10f371189b",
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
                    presumedStudyID = encodeInBase64(
                        study["@id"].replace("https://doi.org/", ""), "utf-8"
                    )

                    studyEntries = [
                        PIDRecordEntry(
                            "21.T11148/d0773859091aeb451528",
                            fdo.getPID(),
                            "hasMetadata",
                        ),
                    ]

                    try:
                        studyPID = addEntries(presumedStudyID, studyEntries)
                        if studyPID is not None:
                            fdo.addEntry(
                                "21.T11148/4fe7cde52629b61e3b82",
                                studyPID,
                                "isMetadataFor",
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
