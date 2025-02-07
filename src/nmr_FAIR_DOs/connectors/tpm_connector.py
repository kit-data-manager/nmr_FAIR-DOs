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
from string import Template

import requests

from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.utils import fetch_multiple

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class TPMConnector:
    """
    This class handles all communication with the Typed PID Maker (TPM) (see https://kit-data-manager.github.io/webpage/typed-pid-maker for more information).
    The Typed PID Maker is a service that allows the creation and management of FAIR-DOs (Findable, Accessible, Interoperable, Reusable Digital Objects).
    It communicates with the Handle.net service to create PIDs and stores PID records in a globally persistent manner.

    Attributes:
        _tpm_url:str The URL of the Typed PID Maker instance
    """

    def __init__(self, tpm_url: str):
        """
        Creates a new TPMConnector

        Args:
            tpm_url:str The URL of the Typed PID Maker instance

        Raises:
            ValueError: If the TPM URL is None or empty
        """
        if tpm_url is None or len(tpm_url) == 0:
            raise ValueError("TPM URL must not be None or empty")
        self._tpm_url = tpm_url

    def createSingleFAIRDO(self, pidRecord: PIDRecord) -> PIDRecord:
        """
        Creates a single FAIR-DO in the TPM

        Args:
            pidRecord:PIDRecord The FAIR-DO to create

        Returns:
            PIDRecord The response from the TPM. This response contains the PID and the PID record.
        """
        logger.info(f"Creating FAIR-DO {pidRecord.getPID()}")

        if pidRecord is None or not isinstance(
            pidRecord, PIDRecord
        ):  # if the PID record is None or not an instance of PIDRecord, raise an error
            raise ValueError(
                "FAIR-DO must not be None and must be an instance of PIDRecord"
            )

        headers = {"Content-Type": "application/json"}

        # content = self._applyTypeAPIFixes(pidRecord.toJSON()) # Possible fix for Type API issues
        content = pidRecord.toJSON()  # get the JSON representation of the PID record

        endpoint = "/api/v1/pit/pid"

        if (
            content is None or len(content) == 0
        ):  # if the content is None or empty, raise an error
            raise ValueError("No content to create due to invalid input")

        resource_response = requests.post(
            self._tpm_url + endpoint, headers=headers, json=content
        )  # send a POST request to the TPM to create the PID record
        logger.debug(f"Response for URL {self._tpm_url + endpoint}", resource_response)

        if (
            resource_response.status_code != 201
        ):  # if the status code is not 201, raise an error
            raise Exception("Error creating PID record: ", resource_response)

        return PIDRecord.fromJSON(
            resource_response.json()
        )  # parse a PID record from the response JSON and return it

    def createMultipleFAIRDOs(self, pidRecord: list[PIDRecord]) -> list[PIDRecord]:
        """
        Creates multiple FAIR-DOs in the TPM.
        This function uses the bulk-create endpoint of the TPM to create multiple FAIR-DOs at once.
        One advantage of using this endpoint is that it can create multiple connected FAIR-DOs in one request and automatically replaces "placeholder"/"fantasy"/"preliminary" PIDs in the records with the real deal.

        Args:
            pidRecord:list[PIDRecord] The FAIR-DOs to create

        Returns:
            list[PIDRecord] The response from the TPM which is a list of all created FAIR-DOs
        """
        logger.info(f"Creating {len(pidRecord)} FAIR-DOs")

        headers = {"Content-Type": "application/json"}

        content = []

        for fairdo in pidRecord:  # iterate over all PID records
            if fairdo is None or not isinstance(
                fairdo, PIDRecord
            ):  # Check the validity or raise an exception
                raise ValueError(
                    "FAIR-DO must not be None and must be an instance of PIDRecord"
                )

            # content.append(self._applyTypeAPIFixes(fairdo.toJSON())) # Possible fix for Type API issues
            content.append(fairdo.toJSON())  # Mark this record ready for creation

        endpoint = "/api/v1/pit/pids"

        if (
            content is None or len(content) == 0
        ):  # if the content is None or empty, raise an error
            raise ValueError("No content to create due to invalid input")

        logger.debug(
            f"Creating FAIR-DOs at {self._tpm_url + endpoint} : {json.dumps(content)[:250]}"
        )
        resource_response = requests.post(
            self._tpm_url + endpoint, headers=headers, json=content, timeout=None
        )  # send a POST request to the TPM to create the PID records

        logger.debug(f"Response for URL {self._tpm_url + endpoint}", resource_response)

        if (
            resource_response.status_code != 201
        ):  # if the status code is not 201, raise an error
            raise Exception(
                "Error creating PID records. API response from TPM: ",
                repr(resource_response),
            )

        result = []
        for (
            i
        ) in resource_response.json():  # iterate over all PID records in the response
            result.append(
                PIDRecord.fromJSON(i)
            )  # parse a PID record from the response JSON and add it to the result list

        logger.info("Successfully created FAIR-DOs")
        return result  # return the list of all created PID records (with their actual PIDs)

    def getPIDRecord(self, pid: str) -> PIDRecord:
        """
        Retrieves a PID record from the TPM

        Args:
            pid (str): The PID to retrieve

        Returns:
            PIDRecord: The PID record retrieved from the TPM

        Raises:
            ValueError: If the PID is None or empty
            Exception: If the PID record cannot be retrieved
        """
        if pid is None or len(pid) == 0:  # if the PID is None or empty, raise an error
            raise ValueError("PID must not be None or empty")

        endpoint = "/api/v1/pit/pid/" + pid

        resource_response = requests.get(
            self._tpm_url + endpoint, headers={"Accept": "application/json"}
        )  # send a GET request to the TPM to retrieve the PID record

        if (
            resource_response.status_code != 200
        ):  # if the status code is not 200, raise an error
            raise Exception("Error retrieving PID record: ", resource_response)

        return PIDRecord.fromJSON(
            resource_response.json()
        )  # parse a PID record from the response JSON and return it

    def updatePIDRecord(self, pidRecord: PIDRecord) -> PIDRecord:
        """
        Updates a PID record in the TPM

        Args:
            pidRecord:PIDRecord The PID record to update

        Returns:
            PIDRecord The response from the TPM

        Raises:
            ValueError: If the PID record is None or not an instance of PIDRecord
            Exception: If the PID record cannot be updated
        """
        if pidRecord is None or not isinstance(
            pidRecord, PIDRecord
        ):  # if the PID record is None or not an instance of PIDRecord, raise an error
            raise ValueError(
                "PID record must not be None and must be an instance of PIDRecord"
            )

        headers = {"Content-Type": "application/json"}

        content = pidRecord.toJSON()  # get the JSON representation of the PID record

        endpoint = "/api/v1/pit/pid/" + pidRecord.getPID()  # create the endpoint URL

        if (
            content is None or len(content) == 0
        ):  # if the content is None or empty, raise an error
            raise ValueError("No content to update due to invalid input")

        resource_response = requests.put(
            self._tpm_url + endpoint, headers=headers, json=content
        )  # send a PUT request to the TPM to update the PID record

        if (
            resource_response.status_code != 200
        ):  # if the status code is not 200, raise an error
            raise Exception(
                "Error updating PID record: ",
                resource_response,
            )

        return PIDRecord.fromJSON(
            resource_response.json()
        )  # parse a PID record from the response JSON and return it

    async def getAllPIDRecords(self) -> list[PIDRecord]:
        """
        Retrieves all PID records from the TPM.
        This function uses the known-pid endpoint of the TPM to retrieve all PID records.
        Keep in mind that the TPM does not necessarily know all PIDs in the system or even in the prefix or same instance.

        Returns:
            list[PIDRecord] The list of all PID records
        """
        endpoint = "/api/v1/pit/known-pid"

        resource_response = requests.get(
            self._tpm_url + endpoint, headers={"Accept": "application/json"}
        )  # send a GET request to the TPM to retrieve all PID records

        if (
            resource_response.status_code != 200
        ):  # if the status code is not 200, raise an error
            raise Exception("Error retrieving PID records: ", resource_response)

        url_template = Template(
            "$tpmURL/api/v1/pit/pid/$pid"
        )  # create a template for the URL

        single_pidRecord_urls = []
        for (
            i
        ) in resource_response.json():  # iterate over all PID records in the response
            # Create the URL
            url = url_template.safe_substitute(
                tpmURL=self._tpm_url,
                pid=i["pid"],
            )  # create the URL for the PID record
            single_pidRecord_urls.append(url)  # add the URL to the list

        # Fetch all PID records in a parallelized manner. Do not use the cache.
        json_records = await fetch_multiple(single_pidRecord_urls, True)

        result = []
        for i in json_records:  # iterate over all PID records in the response
            result.append(
                PIDRecord.fromJSON(i)
            )  # parse a PID record from the response JSON and add it to the result list

        return result  # return the list of all fetched PID records

    @staticmethod
    def _applyTypeAPIFixes(content: dict) -> dict:
        """
        Applies fixes to the content to match the TPM API.
        This is due to an issue in the schema generation of the Type API.

        Args:
            content:dict The content to fix

        Returns:
            dict The fixed content

        Raises:
            ValueError: If the content is None or not a dict
        """
        # Define a set of types that need to be fixed and the new name of the internal key
        types_to_fix = {
            "21.T11969/8710d753ad10f371189b": "landingPageLocation",
            "21.T11148/f3f0cbaa39fa9966b279": "identifier",
            "21.T11969/7a19f6d5c8e63dd6bfcb": "NMR_Method",
            "21.T11148/7fdada5846281ef5d461": "locationPreview/Sample",
        }

        if content is None or not isinstance(
            content, dict
        ):  # if the content is None or not a dict, raise an error
            raise ValueError("Content must not be None and must be a dict")

        result = {"pid": content["pid"], "entries": {}}

        for key, value in content[
            "entries"
        ].items():  # iterate over all entries in the content
            fix_name = types_to_fix.get(key)  # get the fix name for the current key

            if fix_name is not None:  # if a fix name is available
                logger.debug(f"Fixing content for type {key} to {fix_name}")
                values = []
                for item in value:  # iterate over all values to this key
                    newEntry = {
                        "key": item["key"],
                        "value": '{"' + fix_name + '": "' + item["value"] + '"}',
                    }  # create a new entry with the fixed name and value in an internal JSON string
                    values.append(newEntry)  # add the new entry to the list
                result["entries"][key] = values  # define the new entries for the key
                logger.debug(f"Fixed content for type {key} to {values}")
            else:  # if no fix name is available
                result["entries"][key] = value  # use the original value
                logger.debug(f"No fix for type {key}")

        return result  # return the fixed content
