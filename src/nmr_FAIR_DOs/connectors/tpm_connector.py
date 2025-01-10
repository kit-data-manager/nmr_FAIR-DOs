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

import requests

from nmr_FAIR_DOs.domain.pid_record import PIDRecord

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


class TPMConnector:
    def __init__(self, tpm_url: str):
        self._tpm_url = tpm_url

    def createSingleFAIRDO(self, fairdo: PIDRecord) -> PIDRecord:
        """
        Creates a single FAIR-DO in the TPM

        :param fairdo:PIDRecord The FAIR-DO to create

        :return:PIDRecord The response from the TPM
        """
        logger.info(f"Creating FAIR-DO {fairdo.getPID()}")

        if fairdo is None or not isinstance(fairdo, PIDRecord):
            raise ValueError(
                "FAIR-DO must not be None and must be an instance of PIDRecord"
            )

        headers = {"Content-Type": "application/json"}

        content = fairdo.exportJSON()

        endpoint = "api/v1/pit/pid"

        if content is None or len(content) == 0:
            raise ValueError("No content to create due to invalid input")

        resource_response = requests.post(
            self._tpm_url + endpoint, headers=headers, json=content
        )
        logger.debug(f"Response for URL {self._tpm_url + endpoint}", resource_response)

        if resource_response.status_code != 201:
            raise Exception("Error creating PID record: ", resource_response)

        return PIDRecord.fromJSON(resource_response.json())

    def createMultipleFAIRDOs(self, fairdos: list[PIDRecord]) -> list[PIDRecord]:
        """
        Creates multiple FAIR-DOs in the TPM

        :param fairdos:list[PIDRecord] The FAIR-DOs to create

        :return:list[PIDRecord] The response from the TPM which is a list of all created FAIR-DOs
        """
        logger.info(f"Creating {len(fairdos)} FAIR-DOs")

        headers = {"Content-Type": "application/json"}

        content = []

        for fairdo in fairdos:
            if fairdo is None or not isinstance(fairdo, PIDRecord):
                raise ValueError(
                    "FAIR-DO must not be None and must be an instance of PIDRecord"
                )

            content.append(fairdo.exportJSON())

        endpoint = "api/v1/pit/pids"

        if content is None or len(content) == 0:
            raise ValueError("No content to create due to invalid input")

        logger.debug(f"Creating FAIR-DOs at {self._tpm_url + endpoint}", content)
        resource_response = requests.post(
            self._tpm_url + endpoint, headers=headers, json=content
        )

        if resource_response.status_code != 201:
            raise Exception("Error creating PID records: ", resource_response)

        result = []
        for i in resource_response.json():
            result.append(PIDRecord.fromJSON(i))

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

        endpoint = "api/v1/pit/pid/" + pid

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

        content = pidRecord.exportJSON()

        endpoint = "api/v1/pit/pid/" + pidRecord.getPID()

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
