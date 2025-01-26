"""CLI of nmr_FAIR-DOs."""

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
import logging
from datetime import datetime

import typer

from nmr_FAIR_DOs.lib import (
    create_pidRecords_from_scratch,
    getRepositories,
    add_all_existing_pidRecords_to_elasticsearch,
)
from nmr_FAIR_DOs.repositories.AbstractRepository import AbstractRepository

logging.basicConfig()
# logging.basicConfig(filename="nmr_FAIR_DOs.log",
#                     filemode='a',
#                     format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
fh = logging.FileHandler("all.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# create subcommand app
say = typer.Typer()

# create the main app
app = typer.Typer()
app.add_typer(say, name="say")


@app.command()
def createAllAvailable(
    repositories: list[str] = [None],
    start: datetime = None,
    end: datetime = None,
    dryrun: bool = False,
):
    """
    Create PID records for all available resources.
    """
    logger.info(
        f"Creating PID records for all available resources in {repositories} in timerange {start}-{end}. Dryrun: {dryrun}"
    )

    repos: list[AbstractRepository] = getRepositories(repositories)
    resources = asyncio.run(create_pidRecords_from_scratch(repos, start, end, dryrun))

    typer.echo(f"Created PID records for {len(resources)} resources in {repos}.")
    typer.echo("If errors occurred, please see error_*.json for details.")


#
# @app.command()
# def retryErrors(repo: str):
#     """
#     Retry the creation of PID records for the resources that caused errors during the last run.
#     """
#     logger.info(
#         f"Retrying the creation of PID records for the resources that caused errors during the last run in {repo}."
#     )
#
#     repository: AbstractRepository = getRepository(repo)
#     resources = asyncio.run(recreate_pidRecords_with_errors(repository))
#
#     typer.echo(f"Created PID records for {len(resources)} resources in {repo}.")
#     typer.echo(
#         f"If errors occurred, please see error_{repository.repositoryID}.json for details."
#     )


@app.command()
def buildElastic(
    from_file: str = typer.Option(
        None, help="Path to a file containing PID records to be indexed."
    ),
):
    """
    Build the ElasticSearch index for all available resources.
    """
    logger.info("Building the ElasticSearch index for all available resources.")
    asyncio.run(add_all_existing_pidRecords_to_elasticsearch(from_file))


if __name__ == "__main__":
    app()
