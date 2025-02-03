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

import os

from dotenv import load_dotenv

load_dotenv()

TPM_URL = os.getenv("TPM_URL")
CHEMOTION_BASE_URL = os.getenv("CHEMOTION_BASE_URL")
NMRXIV_BASE_URL = os.getenv("NMRXIV_BASE_URL")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX")
ELASTICSEARCH_APIKEY = os.getenv("ELASTICSEARCH_APIKEY")
CACHE_DIR = os.getenv("CACHE_DIR")
TERMINOLOGY_URL = os.getenv("TERMINOLOGY_URL")

# Check if the environment variables are set
if not TPM_URL:
    raise Exception("TPM_URL is not set")
if not CHEMOTION_BASE_URL:
    raise Exception("CHEMOTION_BASE_URL is not set")
if not NMRXIV_BASE_URL:
    raise Exception("NMRXIV_BASE_URL is not set")
if not ELASTICSEARCH_URL:
    raise Exception("ELASTICSEARCH_URL is not set")
if not ELASTICSEARCH_INDEX:
    raise Exception("ELASTICSEARCH_INDEX is not set")
if not ELASTICSEARCH_APIKEY:
    raise Exception("ELASTICSEARCH_APIKEY is not set")
if not CACHE_DIR:
    raise Exception("CACHE_DIR is not set")
if not TERMINOLOGY_URL:
    raise Exception("TERMINOLOGY_URL is not set")

# Check if the cache directory exists
if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# Remove trailing slashes from the URLs if present
if TPM_URL.endswith("/"):
    TPM_URL = TPM_URL[:-1]
if CHEMOTION_BASE_URL.endswith("/"):
    CHEMOTION_BASE_URL = CHEMOTION_BASE_URL[:-1]
if NMRXIV_BASE_URL.endswith("/"):
    NMRXIV_BASE_URL = NMRXIV_BASE_URL[:-1]
if ELASTICSEARCH_URL.endswith("/"):
    ELASTICSEARCH_URL = ELASTICSEARCH_URL[:-1]
if TERMINOLOGY_URL.endswith("/"):
    TERMINOLOGY_URL = TERMINOLOGY_URL[:-1]
