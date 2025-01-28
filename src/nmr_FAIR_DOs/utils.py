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
import asyncio
import base64
import json
import logging
import os.path
from datetime import datetime

import aiohttp

from nmr_FAIR_DOs.env import CACHE_DIR

logger = logging.getLogger(__name__)

known_licenses: dict[str, str] = {}


async def fetch_data(url: str, forceFresh: bool = False) -> dict:
    """
    Fetches data from the specified URL.

    Args:
        url (str): The URL to fetch data from
        forceFresh (bool): Whether to force fetching fresh data. This tells the function to ignore cached data.

    Returns:
        dict: The fetched data

    Raises:
        ValueError: If the URL is invalid or the data cannot be fetched
    """
    if not url or url is None or not isinstance(url, str):
        raise ValueError("Invalid URL")

    filename = CACHE_DIR + "/" + url.replace("/", "_") + ".json"

    # check if data is cached
    if os.path.isfile(filename) and not forceFresh:
        with open(filename, "r") as f:
            result = json.load(f)
            if result is not None and isinstance(result, dict):
                logger.info(f"Using cached data for {url}")
                return result

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(filename, "w") as f:  # save to cache
                        json.dump(await response.json(), f)
                    return await response.json()
                else:
                    logger.error(f"Failed to fetch {url}: {response.status}", response)
                    raise ValueError(
                        f"Failed to fetch {url}: {response.status}",
                        response,
                        datetime.now().isoformat(),
                    )
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        raise ValueError(str(e), url, datetime.now().isoformat())


async def fetch_multiple(urls: list[str], forceFresh: bool = False) -> list[dict]:
    """
    Fetches data from multiple URLs.

    Args:
        urls (List[str]): A list of URLs to fetch data from
        forceFresh (bool): Whether to force fetching fresh data. This tells the function to ignore cached data.

    Returns:
        List[dict]: A list of fetched data

    Raises:
        ValueError: If the URLs are invalid or the data cannot be fetched
    """
    if not urls or urls is None or not isinstance(urls, list):
        raise ValueError("Invalid URLs. Please provide a list of URLs.")

    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector):
        results = []
        for i in range(0, len(urls), 100):
            batch = urls[i : i + 100]
            tasks = [asyncio.create_task(fetch_data(url, forceFresh)) for url in batch]
            results.extend(await asyncio.gather(*tasks))
        return results


def encodeInBase64(data: str) -> str:
    """
    Encodes the given data in Base64.

    Args:
        data (str): The data to encode

    Returns:
        str: The Base64 encoded data

    Raises:
        ValueError: If the data is None or empty
    """
    if data is None or len(data) == 0:
        raise ValueError("Data must not be None or empty")

    result = base64.b64encode(bytes(data, "utf-8")).decode("utf-8")
    return result


def decodeFromBase64(data: str) -> str:
    """
    Decodes the given Base64 encoded data.

    Args:
        data (str): The Base64 encoded data to decode

    Returns:
        str: The decoded data

    Raises:
        ValueError: If the data is None or empty
    """
    if data is None or len(data) == 0:
        raise ValueError("Data must not be None or empty")

    result = base64.b64decode(data).decode("utf-8")
    return result


def parseDateTime(text: str) -> datetime:
    """
    Parses a datetime from an arbitrary string.

    Args:
        text (str): The string to parse

    Returns:
        datetime: The parsed datetime

    Raises:
        ValueError: If the text is None or empty or the datetime cannot be parsed
    """
    if text is None or len(text) == 0:
        raise ValueError("Text must not be None or empty")

    ## Support none-ISO8601 datetime formats
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        pass

    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        pass

    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        pass

    raise ValueError("Could not parse datetime from text " + text)


async def parseSPDXLicenseURL(input_str: str) -> str:
    """
    Parses a license URL to an SPDX license url.

    Args:
        input_str (str): The license URL to parse

    Returns:
        str: The SPDX license URL
    """
    spdx_base_url = "https://spdx.org/licenses/"
    format = "json"

    if input_str in known_licenses:
        logger.debug(
            f"Using cached license URL for {input_str}: {known_licenses[input_str]}"
        )
        return known_licenses[input_str]

    # Fetch the list of licenses once
    licenses = await fetch_data(f"{spdx_base_url}licenses.json")
    licenses = licenses["licenses"]

    for license in licenses:  # iterate over the licenses
        url = f"{spdx_base_url}{license['licenseId']}.{format}"  # create the URL

        if (
            input_str.lower() == license["reference"].lower()
        ):  # check if the input string is the reference (e.g. https://spdx.org/licenses/MIT.html)
            known_licenses[input_str] = url
            return url
        elif (
            input_str.lower() in license["details"].lower()
        ):  # check if the input string is in the details (e.g. https://spdx.org/licenses/MIT.json)
            known_licenses[input_str] = url
            return url
        elif (
            input_str.lower() == license["licenseId"].lower()
        ):  # check if the input string is the license ID (e.g. MIT)
            known_licenses[input_str] = url
            return url
        elif (
            input_str.lower() in license["seeAlso"]
        ):  # check if the input string is in the seeAlso (e.g. https://opensource.org/license/mit/)
            known_licenses[input_str] = url
            return url
        elif (
            input_str.lower() == license["name"].lower()
        ):  # check if the input string is in the name (e.g. MIT License)
            known_licenses[input_str] = url
            return url
        elif input_str == str(
            license["referenceNumber"]
        ):  # check if the input string is the reference number (e.g. 1)
            known_licenses[input_str] = url
            return url

    logger.warning(f"Could not parse license URL {input_str}")
    return input_str
