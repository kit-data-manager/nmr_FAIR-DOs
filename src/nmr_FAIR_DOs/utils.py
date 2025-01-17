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
import base64
import asyncio
import json
import logging
import os.path
from datetime import datetime

import aiohttp
from nmr_FAIR_DOs.env import CACHE_DIR

logger = logging.getLogger(__name__)


async def fetch_data(url: str, forceFresh: bool = False):
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


async def fetch_multiple(urls: list[str], forceFresh: bool = False):
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
