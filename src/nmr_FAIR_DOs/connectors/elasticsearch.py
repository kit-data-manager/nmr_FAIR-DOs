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
from elasticsearch.helpers import bulk

from nmr_FAIR_DOs.domain.dataType import extractDataTypeNameFromPID
from nmr_FAIR_DOs.domain.pid_record import PIDRecord
from nmr_FAIR_DOs.domain.pid_record_entry import PIDRecordEntry

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

always_as_list = ["isMetadataFor", "hasMetadata", "contact"]


async def _generate_elastic_JSON_from_PIDRecord(pidRecord):
    result: dict = {"pid": pidRecord.getPID()}

    def addToResult(human_readable_key: str, value_to_add: str):
        """
        Adds a key and a value to the result. If the key already exists in the result, the value is added to a list.
        If the key is in the always_as_list list, the value is always as a list.

        Args:
            human_readable_key (str): The key to add to the result
            value_to_add (str): The value to add to the result
        """
        if human_readable_key in result:
            if isinstance(
                result[human_readable_key], list
            ):  # if the value is already a list
                result[human_readable_key].append(
                    value_to_add
                )  # add the value to the existing list
            else:
                result[
                    human_readable_key
                ] = [  # create a list with the existing value and the new value
                    result[human_readable_key],
                    value_to_add,
                ]  # create a list with the existing value and the new value
        elif (
            human_readable_key in always_as_list
        ):  # for some keys, the value should always be a list (e.g. isMetadataFor, hasMetadata).
            logger.debug(
                f"Adding {human_readable_key} as list to result as it is in always_as_list"
            )
            result[human_readable_key] = [value_to_add]  # create a list with the value
        else:
            result[human_readable_key] = value_to_add  # create a list with the value

    # Extract the entries from the PID record
    for attribute, value in pidRecord.getEntries().items():
        key = await extractDataTypeNameFromPID(attribute)
        for i in value:  # iterate over the values of the PID record entry
            if isinstance(i, PIDRecordEntry):  # if the value is a PIDRecordEntry
                if isinstance(
                    i.value, dict
                ):  # if the value of the PIDRecordEntry is a dict
                    for k, v in i.value.items():  # iterate over the dict
                        if v is None:  # if the value is None, continue
                            continue

                        kString = f"{key}.{await extractDataTypeNameFromPID(k)}"  # create a key string by concatenating the key and the extracted data type name from the PID
                        addToResult(
                            kString, v
                        )  # add the key string and the value to the result
                else:  # if the value of the PIDRecordEntry is not a dict (i.e. a string)
                    addToResult(key, i.value)  # add the key and the value to the result
            else:
                addToResult(key, i["value"])  # add the key and the value to the result

    # Extract the timestamp from the PID record or use the current time as timestamp
    if pidRecord.entryExists("21.T11148/aafd5fb4c7222e2d950a"):
        result["timestamp"] = pidRecord.getEntries()["21.T11148/aafd5fb4c7222e2d950a"][
            0
        ].value
    else:
        result["timestamp"] = datetime.now().isoformat()
    return result


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

        logger.info(f"Connected to Elasticsearch: {self._client.info()}")

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
        result = await _generate_elastic_JSON_from_PIDRecord(pidRecord)

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
        # bulk insert the PID records into the index with the elastic bulk API

        actions = [
            {
                "_op_type": "create",
                "_index": self._indexName,
                "_id": pidRecord.getPID(),
                "_source": await _generate_elastic_JSON_from_PIDRecord(pidRecord),
            }
            for pidRecord in pidRecords
        ]

        response = bulk(self._client, actions)
        logger.debug(
            "Elasticsearch response for bulk insert of PID records: ", response
        )

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
