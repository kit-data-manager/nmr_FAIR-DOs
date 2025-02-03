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
    def __init__(self, tpm_url: str):
        self._tpm_url = tpm_url

    def createSingleFAIRDO(self, pidRecord: PIDRecord) -> PIDRecord:
        """
        Creates a single FAIR-DO in the TPM

        :param pidRecord:PIDRecord The FAIR-DO to create

        :return:PIDRecord The response from the TPM
        """
        logger.info(f"Creating FAIR-DO {pidRecord.getPID()}")

        if pidRecord is None or not isinstance(pidRecord, PIDRecord):
            raise ValueError(
                "FAIR-DO must not be None and must be an instance of PIDRecord"
            )

        headers = {"Content-Type": "application/json"}

        # content = self._applyTypeAPIFixes(pidRecord.toJSON()) # TODO: Fix this
        content = pidRecord.toJSON()

        endpoint = "/api/v1/pit/pid"

        if content is None or len(content) == 0:
            raise ValueError("No content to create due to invalid input")

        resource_response = requests.post(
            self._tpm_url + endpoint, headers=headers, json=content
        )
        logger.debug(f"Response for URL {self._tpm_url + endpoint}", resource_response)

        if resource_response.status_code != 201:
            raise Exception("Error creating PID record: ", resource_response)

        return PIDRecord.fromJSON(resource_response.json())

    def createMultipleFAIRDOs(self, pidRecord: list[PIDRecord]) -> list[PIDRecord]:
        """
        Creates multiple FAIR-DOs in the TPM

        :param pidRecord:list[PIDRecord] The FAIR-DOs to create

        :return:list[PIDRecord] The response from the TPM which is a list of all created FAIR-DOs
        """
        logger.info(f"Creating {len(pidRecord)} FAIR-DOs")

        headers = {"Content-Type": "application/json"}

        content = []

        for fairdo in pidRecord:
            if fairdo is None or not isinstance(fairdo, PIDRecord):
                raise ValueError(
                    "FAIR-DO must not be None and must be an instance of PIDRecord"
                )

            # content.append(self._applyTypeAPIFixes(fairdo.toJSON())) # TODO: Fix this
            content.append(fairdo.toJSON())

        endpoint = "/api/v1/pit/pids"

        if content is None or len(content) == 0:
            raise ValueError("No content to create due to invalid input")

        logger.debug(json.dumps(content))

        # logger.debug(
        #     f"Creating FAIR-DOs at {self._tpm_url + endpoint}",
        #     json.dumps(content)[:250],
        # )
        resource_response = requests.post(
            self._tpm_url + endpoint, headers=headers, json=content, timeout=None
        )

        logger.debug(f"Response for URL {self._tpm_url + endpoint}", resource_response)

        if resource_response.status_code != 201:
            raise Exception(
                "Error creating PID records. API response from TPM: ",
                repr(resource_response),
            )

        result = []
        for i in resource_response.json():
            result.append(PIDRecord.fromJSON(i))

        logger.info("Successfully created FAIR-DOs")
        return result

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
        if pid is None or len(pid) == 0:
            raise ValueError("PID must not be None or empty")

        endpoint = "/api/v1/pit/pid/" + pid

        resource_response = requests.get(
            self._tpm_url + endpoint, headers={"Accept": "application/json"}
        )

        if resource_response.status_code != 200:
            raise Exception("Error retrieving PID record: ", resource_response)

        return PIDRecord.fromJSON(resource_response.json())

    def updatePIDRecord(self, pidRecord: PIDRecord) -> PIDRecord:
        """
        Updates a PID record in the TPM

        :param pidRecord:PIDRecord The PID record to update

        :return:PIDRecord The response from the TPM
        """
        if pidRecord is None or not isinstance(pidRecord, PIDRecord):
            raise ValueError(
                "PID record must not be None and must be an instance of PIDRecord"
            )

        headers = {"Content-Type": "application/json"}

        content = pidRecord.toJSON()

        endpoint = "/api/v1/pit/pid/" + pidRecord.getPID()

        if content is None or len(content) == 0:
            raise ValueError("No content to update due to invalid input")

        resource_response = requests.put(
            self._tpm_url + endpoint, headers=headers, json=content
        )

        if resource_response.status_code != 200:
            raise Exception(
                "Error updating PID record: ",
                resource_response,
            )

        return PIDRecord.fromJSON(resource_response.json())

    async def getAllPIDRecords(self) -> list[PIDRecord]:
        """
        Retrieves all PID records from the TPM

        :return:list[PIDRecord] The list of all PID records
        """
        endpoint = "/api/v1/pit/known-pid"

        resource_response = requests.get(
            self._tpm_url + endpoint, headers={"Accept": "application/json"}
        )

        if resource_response.status_code != 200:
            raise Exception("Error retrieving PID records: ", resource_response)

        url_template = Template("$tpmURL/api/v1/pit/pid/$pid")

        single_pidRecord_urls = []
        for i in resource_response.json():
            # Create the URL
            url = url_template.safe_substitute(
                tpmURL=self._tpm_url,
                pid=i["pid"],
            )
            single_pidRecord_urls.append(url)

        json_records = await fetch_multiple(single_pidRecord_urls, True)

        result = []
        for i in json_records:
            result.append(PIDRecord.fromJSON(i))

        return result

    def _applyTypeAPIFixes(self, content: dict) -> dict:
        """
        Applies fixes to the content to match the TPM API

        :param content:dict The content to fix

        :return:dict The fixed content
        """
        types_to_fix = {
            "21.T11969/8710d753ad10f371189b": "landingPageLocation",
            "21.T11148/f3f0cbaa39fa9966b279": "identifier",
            "21.T11969/7a19f6d5c8e63dd6bfcb": "NMR_Method",
            "21.T11148/7fdada5846281ef5d461": "locationPreview/Sample",
        }

        if content is None or not isinstance(content, dict):
            raise ValueError("Content must not be None and must be a dict")

        result = {"pid": content["pid"], "entries": {}}

        for key, value in content["entries"].items():
            fix_name = types_to_fix.get(key)

            if fix_name is not None:
                logger.debug(f"Fixing content for type {key} to {fix_name}")
                values = []
                for item in value:
                    newEntry = {
                        "key": item["key"],
                        "value": '{"' + fix_name + '": "' + item["value"] + '"}',
                    }
                    # logger.debug(f"Fixed content for type {item["key"]} to {newEntry}")
                    values.append(newEntry)
                result["entries"][key] = values
                logger.debug(f"Fixed content for type {key} to {values}")
            else:
                result["entries"][key] = value
                logger.debug(f"No fix for type {key}")

        return result
