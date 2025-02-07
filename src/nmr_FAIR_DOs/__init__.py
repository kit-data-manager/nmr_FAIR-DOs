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

from importlib.metadata import version
from typing import Final

# Set version, it will use version from pyproject.toml if defined
__version__: Final[str] = version(__package__ or __name__)

"""
This package contains the source code of the project.

The package is structured as follows:

- `connectors/`: Contains the connectors to the different data sources.
- `domain/`: Contains the domain classes of the project.
- `repositories/`: Contains the repositories for which FAIR-DOs are generated.
- `cli.py`: Contains the command line interface of the project.
- `env.py`: Manages the environment variables of the project.
- `lib.py`: Contains the main business logic of the project.
- `utils.py`: Contains various utility functions.
"""
