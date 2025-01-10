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

from fastapi import FastAPI

from nmr_FAIR_DOs.lib import (
    getRepository,
    create_pidRecords_from_scratch,
    recreate_pidRecords_with_errors,
)
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository

app = FastAPI()


@app.get("/createAll/{repo}")
def createAllAvailable(repo: str):
    """
    Create PID records for all available resources.
    """
    repository: AbstractRepository = getRepository(repo)
    resources = asyncio.run(create_pidRecords_from_scratch(repository))

    print(f"Created PID records for {len(resources)} resources in {repo}.")
    print(
        f"If errors occurred, please see error_{repository.repositoryID}.json for details. You can retry the creation of PID records for the failed resources by using the 'retryErrors' command."
    )
    return resources


@app.get("/retry/{repo}")
def retryErrors(repo: str):
    """
    Retry the creation of PID records for the resources that caused errors during the last run.
    """
    repository: AbstractRepository = getRepository(repo)
    resources = asyncio.run(recreate_pidRecords_with_errors(repository))
    print(f"Created PID records for {len(resources)} resources in {repo}.")
    print(
        f"If errors occurred, please see error_{repository.repositoryID}.json for details."
    )
    return resources


# @app.get("/calculate/{op}")
# def calc(op: CalcOperation, x: int, y: int = 0):
#     """Return result of calculation on two integers."""
#     try:
#         return calculate(op, x, y)
#
#     except (ZeroDivisionError, ValueError, NotImplementedError) as e:
#         if isinstance(e, ZeroDivisionError):
#             err = f"Cannot divide x={x} by y=0!"
#         else:
#             err = str(e)
#         raise HTTPException(status_code=422, detail=err)


def run():
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    run()
