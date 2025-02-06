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
    repositories: list[str] = typer.Option(
        None,
        help="List of repositories to create PID records for. If unspecified, all available repositories are queried.",
    ),
    start: datetime = typer.Option(
        None,
        help="Start of the time range to create PID records for. If unspecified, datetime.min() is used.",
    ),
    end: datetime = typer.Option(
        None,
        help="End of the time range to create PID records for. If unspecified, datetime.max() is used.",
    ),
    dryrun: bool = typer.Option(
        False,
        help="If True, only print the resources that would be created. If unspecified or false, create the PID records.",
    ),
):
    """
    Create PID records for all available resources.

    Args:
        repositories (list[str]): List of repositories to create PID records for. If None, all available repositories are queried.
        start (datetime): Start of the time range to create PID records for. If None, datetime.min() is used.
        end (datetime): End of the time range to create PID records for. If None, datetime.max() is used.
        dryrun (bool): If True, only print the resources that would be created. If False, create the PID records. Default: False.
    """
    if repositories is None:
        repositories = [None]
    logger.info(
        f"Creating PID records for all available resources in {repositories} in timerange {start}-{end}. Dryrun: {dryrun}"
    )

    repos: list[AbstractRepository] = getRepositories(repositories)
    resources = asyncio.run(create_pidRecords_from_scratch(repos, start, end, dryrun))

    typer.echo(f"Created PID records for {len(resources)} resources in {repos}.")
    typer.echo("If errors occurred, please see the logs for details.")


@app.command()
def buildElastic(
    from_file: str = typer.Option(
        None,
        help="Path to a file containing PID records to be indexed. If unspecified, all FAIR-DOs in the active Typed PID-Maker instance will be re-indexed.",
    ),
):
    """
    Build the ElasticSearch index for all available resources.

    Args:
        from_file (str): Path to a file containing PID records
            to be indexed. If None, all FAIR-DOs in the active Typed PID-Maker instance will be re-indexed. Default: None.
    """
    logger.info("Building the ElasticSearch index for all available resources.")
    asyncio.run(add_all_existing_pidRecords_to_elasticsearch(from_file))

    typer.echo("ElasticSearch index built successfully.")
