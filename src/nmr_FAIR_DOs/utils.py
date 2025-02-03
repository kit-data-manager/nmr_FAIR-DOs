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
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

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
    Parses a available_license URL to an SPDX available_license url.

    Args:
        input_str (str): The available_license URL to parse

    Returns:
        str: The SPDX available_license URL
    """
    spdx_base_url = "https://spdx.org/licenses"
    file_format = "json"

    if input_str in known_licenses:
        logger.debug(
            f"Using cached available_license URL for {input_str}: {known_licenses[input_str]}"
        )
        return known_licenses[input_str]

    # Fetch the list of licenses once
    available_licenses = await fetch_data(f"{spdx_base_url}/licenses.json")
    available_licenses = available_licenses["licenses"]

    for available_license in available_licenses:  # iterate over the licenses
        url = f"{spdx_base_url}/{available_license['licenseId']}.{file_format}"  # create the URL

        if (
            "reference" in available_license
            and input_str.lower() == available_license["reference"].lower()
        ):  # check if the input string is the reference (e.g. https://spdx.org/licenses/MIT.html)
            known_licenses[input_str] = url
            return url
        elif (
            "details" in available_license
            and input_str.lower() in available_license["details"].lower()
        ):  # check if the input string is in the details (e.g. https://spdx.org/licenses/MIT.json)
            known_licenses[input_str] = url
            return url
        elif (
            "licenseId" in available_license
            and input_str.lower() == available_license["licenseId"].lower()
        ):  # check if the input string is the available_license ID (e.g. MIT)
            known_licenses[input_str] = url
            return url
        elif "seeAlso" in available_license and checkTextIsSimilar(
            input_str, available_license["seeAlso"]
        ):
            # check if the input string is in the seeAlso list (e.g. [https://opensource.org/license/mit/])
            known_licenses[input_str] = url
            return url
        elif "name" in available_license and checkTextIsSimilar(
            input_str, available_license["name"]
        ):  # check if the input string is in the name (e.g. MIT License)
            known_licenses[input_str] = url
            return url
        elif "referenceNumber" in available_license and input_str == str(
            available_license["referenceNumber"]
        ):  # check if the input string is the reference number (e.g. 1)
            known_licenses[input_str] = url
            return url

    logger.warning(f"Could not parse available_license URL {input_str}")
    return input_str


def checkTextIsSimilar(original: str, target: list[str] | str) -> bool:
    """
    Checks if the original text is similar to the target text.

    Args:
        original (str): The original text
        target (list[str]|str): The target text or a list of target texts

    Returns:
        bool: Whether the original text is similar to the target text
    """
    if isinstance(target, str):
        target = [target]

    for t in target:
        # Remove case sensitivity
        original = original.lower()
        t = t.lower()

        # remove whitespaces and prefixes from URLs
        original = original.replace(" ", "")
        t = t.replace(" ", "")
        original = original.replace("https://", "")
        t = t.replace("https://", "")
        original = original.replace("http://", "")
        t = t.replace("http://", "")
        original = original.replace("www.", "")
        t = t.replace("www.", "")
        original = original.replace("legalcode", "")
        t = t.replace("legalcode", "")

        # remove file extensions
        original = original.replace(".json", "")
        t = t.replace(".json", "")
        original = original.replace(".html", "")
        t = t.replace(".html", "")
        original = original.replace(".txt", "")
        t = t.replace(".txt", "")
        original = original.replace(".md", "")
        t = t.replace(".md", "")
        original = original.replace(".xml", "")
        t = t.replace(".xml", "")
        original = original.replace(".rdf", "")
        t = t.replace(".rdf", "")

        # replace licenses with license to match SPDX URLs (e.g. https://opensource.org/licenses/MIT)
        original = original.replace("licenses", "license")
        t = t.replace("licenses", "license")

        # if there is a slash at the end of the URL, remove it
        if original.endswith("/"):
            original = original[:-1]
        if t.endswith("/"):
            t = t[:-1]

        if original == t:  # check if the strings are equal
            logger.debug(f"Found similar text: {original} == {t}")
            return True
        else:
            logger.debug(f"Found different text: {original} != {t}")

    return False


if __name__ == "__main__":
    print(asyncio.run(parseSPDXLicenseURL("https://opensource.org/licenses/MIT")))
    print(asyncio.run(parseSPDXLicenseURL("MIT")))
    print(
        asyncio.run(
            parseSPDXLicenseURL(
                "https://creativecommons.org/licenses/by-sa/4.0/legalcode"
            )
        )
    )

    print("This module is not meant to be executed directly.")
    print("Please import the module and use its functions.")
    exit(1)
