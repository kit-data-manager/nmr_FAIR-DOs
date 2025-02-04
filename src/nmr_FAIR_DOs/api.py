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
)
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository

app = FastAPI()


@app.get("/createAll/{repositories}")
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


# @app.command() TODO: Implement this in the FastAPI app
# def createAllAvailable(
#     repositories: list[str] = [None],
#     start: datetime = None,
#     end: datetime = None,
#     dryrun: bool = False,
# ):
#     """
#     Create PID records for all available resources.
#     """
#     logger.info(
#         f"Creating PID records for all available resources in {repositories} in timerange {start}-{end}. Dryrun: {dryrun}"
#     )
#
#     repos: list[AbstractRepository] = getRepositories(repositories)
#     resources = asyncio.run(create_pidRecords_from_scratch(repos, start, end, dryrun))
#
#     typer.echo(f"Created PID records for {len(resources)} resources in {repos}.")
#     typer.echo("If errors occurred, please see error_*.json for details.")


def run():
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)


if __name__ == "__main__":
    run()
