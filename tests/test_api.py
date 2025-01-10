"""Test API."""

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

from fastapi.testclient import TestClient

from nmr_FAIR_DOs.api import app

client = TestClient(app)

#
# def test_calculate():
#     response = client.get("/calculate/divide?x=5&y=2")
#     assert response.status_code == 200
#     assert response.json() == 2  # int division
#
#     response = client.get("/calculate/divide?x=5&y=0")
#     assert response.status_code == 422
#     assert "y=0" in response.json()["detail"]  # division by 0
#
#     response = client.get("/calculate/add?x=3.14")
#     assert response.status_code == 422  # float input
#
#     response = client.get("/calculate/power?x=5")
#     assert response.status_code == 422  # unsupported op
