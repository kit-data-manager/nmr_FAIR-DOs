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

from elasticsearch import Elasticsearch

from nmr_FAIR_DOs.domain.dataType import extractDataTypeNameFromPID
from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


class ElasticsearchConnector:
    def __init__(self, url: str, apikey: str, indexName: str):
        """
        Creates an Elasticsearch connector

        Args:
            url (str): The URL of the Elasticsearch instance
            apikey (str): The API key to access the Elasticsearch instance
            indexName (str): The name of the index to use
        """

        self._url = url
        self._apikey = apikey
        self._indexName = indexName

        self._client = Elasticsearch(hosts=self._url, api_key=self._apikey)

        logger.info("Connected to Elasticsearch", self._client.info().__repr__())

        # Check if the client is connected
        if not self._client.ping():
            raise Exception("Could not connect to Elasticsearch")

        # Create the index if it does not exist
        if self._client.indices.exists(index=indexName):
            logger.info("Index " + indexName + " already exists")
        else:
            self._client.indices.create(index=indexName)
            logger.info("Created index " + indexName)

    async def addPIDRecord(self, pidRecord: PIDRecord):
        result: dict = {"pid": pidRecord.getPID()}

        # Extract the entries from the PID record
        for attribute, value in pidRecord.getEntries().items():
            # Extract the key
            key = await extractDataTypeNameFromPID(attribute)

            values = [
                {"value": i.value if isinstance(i, PIDRecordEntry) else i["value"]}
                for i in value
            ]

            # Store the values in the result
            result[key] = values if len(values) > 1 else values[0]

        # Extract the timestamp from the PID record or use the current time as timestamp
        if pidRecord.entryExists("21.T11148/aafd5fb4c7222e2d950a"):
            result["timestamp"] = pidRecord.getEntries()[
                "21.T11148/aafd5fb4c7222e2d950a"
            ][0].value
        else:
            result["timestamp"] = datetime.now().isoformat()

        response = self._client.index(
            index=self._indexName, id=result["pid"], document=result
        )

        if response.meta.status not in [200, 201]:
            logger.error(
                "Error storing FAIR-DO in elasticsearch index: " + result["pid"],
                result,
                response,
            )

        logger.info(
            "Stored FAIR-DO in elasticsearch index: " + result["pid"], result, response
        )

    async def addPIDRecords(self, pidRecords: list[PIDRecord]):
        for pidRecord in pidRecords:
            await self.addPIDRecord(pidRecord)

    def searchForPID(self, presumedPID: str) -> str:
        response = self._client.search(
            index=self._indexName,
            body={
                "query": {
                    "multi_match": {
                        "type": "best_fields",
                        "query": presumedPID,
                        # "query": "10.14272/RAXXELZNTBOGNW-UHFFFAOYSA-N/CHMO0000595",
                        "fields": ["digitalObjectLocation", "pid"],
                    }
                }
            },
        )

        logger.debug(
            "Elasticsearch response for search query: " + presumedPID, response
        )

        if response.meta.status != 200:
            logger.error(
                "Error retrieving FAIR-DO from elasticsearch index: " + presumedPID,
                response,
            )
            raise Exception(
                "Error retrieving FAIR-DO from elasticsearch index: " + presumedPID,
                response,
            )

        result = (
            response["hits"]["hits"][0]["_source"]
            if response["hits"]["total"]["value"] > 0
            else None
        )

        if result is None:
            logger.warning(
                "No FAIR-DO found in elasticsearch index: " + presumedPID, response
            )
            raise Exception(
                "No FAIR-DO found in elasticsearch index: " + presumedPID, response
            )
        elif (
            result["pid"] != presumedPID
            and result["digitalObjectLocation"] != presumedPID
        ):
            logger.warning(
                "PID of retrieved FAIR-DO does not match requested PID: " + presumedPID,
                result,
            )
            raise Exception(
                "PID of retrieved FAIR-DO does not match requested PID: " + presumedPID,
                result,
            )

        pid = result["pid"]
        logger.info(
            "Retrieved possible FAIRDO from elasticsearch index: " + pid, result
        )

        return pid
