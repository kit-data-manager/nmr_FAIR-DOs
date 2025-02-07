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

"""
This module contains the domain classes of the nmr_FAIR_DOs package.

- `nmr_FAIR_DOs.domain.PIDRecord`: This class represents a PID record.
- `nmr_FAIR_DOs.domain.PIDRecordEntry`: This class represents a single entry in a PID record. It is heavily used by the PIDRecord class.

Additionally, this module contains a helper function to extract the data type name from a PID. See `nmr_FAIR_DOs.domain.dataType.extractDataTypeNameFromPID`.
"""
