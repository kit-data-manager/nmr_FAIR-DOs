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
from string import Template

import requests
from typing_extensions import Callable

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f"{__name__}.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class Terminology:
    # This list contains the terms that are already in the cache. The format is "query", "IRI". The provided entries were found by hand and are not guaranteed to be correct. Refer to https://www.sigmaaldrich.com/DE/de/technical-documents/technical-article/analytical-chemistry/nuclear-magnetic-resonance/nmr-deuterated-solvent-properties-reference
    cache: dict[str, str] = {
        "DMSO": "http://purl.obolibrary.org/obo/CHEBI_193041",
        "DMSO_D6": "http://purl.obolibrary.org/obo/CHEBI_193041",
        "CDCL3": "http://purl.obolibrary.org/obo/CHEBI_85365",
        "CHLOROFORM-D": "http://purl.obolibrary.org/obo/CHEBI_85365",
        "Acetone": "http://purl.obolibrary.org/obo/CHEBI_78217",
        "Aceton": "http://purl.obolibrary.org/obo/CHEBI_78217",
        "MEOD": "http://purl.obolibrary.org/obo/CHEBI_156265",
        "D2O": "http://purl.obolibrary.org/obo/CHEBI_41981",
        "C6D6": "http://purl.obolibrary.org/obo/CHEBI_193039",
        "CD3CN": "http://purl.obolibrary.org/obo/CHEBI_193038",
        "THF": "http://purl.obolibrary.org/obo/CHEBI_193047",
        "CD2Cl2": "http://purl.obolibrary.org/obo/CHEBI_193042",
        # "MeOH": "http://purl.obolibrary.org/obo/CHEBI_17790"
        # "Dioxane": "http://purl.obolibrary.org/obo/CHEBI_46923"
    }
    validation_functions: dict[str, Callable[[dict], bool]] = {
        "chebi": lambda x: Terminology._validateCHEBI(x)
    }

    def __init__(self, terminology_url: str):
        self._terminology_url = terminology_url

    async def searchForTerm(
        self,
        query: str,
        ontology: str,
        parent: str | None,
        validateNode: Callable[[dict], bool] = None,
    ) -> str | None:
        """
        Searches for a term in the terminology service

        :param query:str The term to search for
        :param ontology:str The ontology to search in
        :param parent:str The parent term to search for
        :param validateNode:Callable[[dict], bool] A function to validate the node found. Input is the entity from the terminology service

        :return:str|None The IRI of the (best) term found
        """
        # Set the validation function to the one provided or the default one for the ontology. If the ontology is not in the list, use a lambda function that always returns True
        validateNode = (
            validateNode
            if validateNode is not None
            else self.validation_functions[ontology]
            if ontology in self.validation_functions
            else lambda x: True
        )

        logger.debug(
            f"Searching for term {query} in ontology {ontology} with parent {parent}"
        )

        # Check if the term is already in the cache
        if query in self.cache:
            logger.debug(f"Found term {query} in cache")
            return self.cache[query]

        # use a url template to replace the placeholders with the actual values
        template = Template(
            "$terminology_url/api/search?q=$query&ontology=$ontology&option=COMPOSITE&fieldList=iri%2Clabel%2Cshort_form%2Cobo_id%2Contology_name&exact=true&obsoletes=false&local=true&allChildrenOf=$parent&rows=10&start=0&format=json&lang=en"
        )
        url = template.substitute(
            terminology_url=self._terminology_url,
            query=query,
            ontology=ontology,
            parent=parent.replace(":", "%3A").replace("/", "%2F")
            if parent is not None
            else "",
        )
        logger.debug(f"URL: {url}")
        response = requests.get(url)

        json = None  # JSON response from the terminology service
        if response.status_code == 200:
            json = response.json()
        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
            raise Exception(f"Error: {response.status_code} - {response.text}")

        entities = []  # List of entities found
        if (
            "response" not in json
            or "docs" not in json["response"]
            or len(json["response"]["docs"]) == 0
        ):
            logger.error(
                f"No results found for query {query} in ontology {ontology} with parent {parent}"
            )
            return None
        else:
            for doc in json["response"]["docs"]:
                iri = doc["iri"]
                entity = await self._getEntity(ontology, iri)

                if entity is not None and validateNode(entity):
                    entities.append(iri)  # Add the IRI to the list of entities found
                else:
                    logger.debug(f"Entity {iri} is not valid and will be ignored")

        if len(entities) == 1:  # If only one entity was found, return it
            logger.info(f"Found single result: {entities[0]}")
            self.cache[query] = entities[0]
            return entities[0]

        result = await self._findParent(
            ontology, entities
        )  # Find the parent of the entities in the search
        if result is None:
            logger.error(
                f"No parent found for entities {entities} in ontology {ontology}"
            )
            return None
        else:
            logger.info(f"Found result to search: {result}")
            self.cache[query] = result
            return result

    async def _getEntity(self, ontology: str, iri: str) -> dict | None:
        """
        Gets an entity from the terminology service

        :param iri:str The IRI of the entity to get

        :return:dict|None The response from the terminology service
        """

        logger.debug(f"Getting entity {iri} from ontology {ontology}")

        iri = iri.replace(":", "%253A").replace(
            "/", "%252F"
        )  # Replace the : and / in the IRI
        url = f"{self._terminology_url}/api/v2/ontologies/{ontology}/entities/{iri}"

        response = requests.get(url)

        if response.status_code == 200:
            return response.json()

        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
            raise Exception(f"Error: {response.status_code} - {response.text}")

    async def _getChildren(self, ontology: str, entity_iri: str) -> list[str]:
        """
        Gets the children of an entity from the terminology service

        :param ontology str The ontology to get the children from
        :param entity_iri:str The IRI of the entity to get the children of

        :return:list[dict] The response from the terminology service
        """
        logger.debug(
            f"Getting children of entity {entity_iri} from ontology {ontology}"
        )

        entity_iri = entity_iri.replace(":", "%253A").replace("/", "%252F")
        url = f"{self._terminology_url}/api/ontologies/{ontology}/terms/{entity_iri}/hierarchicalChildren?lang=en"

        logger.debug(f"Getting children from URL {url}")
        response = requests.get(url)

        children: list[str] = []
        if response.status_code == 200:
            json = response.json()

            if "_embedded" not in json or "terms" not in json["_embedded"]:
                logger.error(
                    f"No children found for entity {entity_iri} from ontology {ontology}"
                )
                return children
            else:
                for term in json["_embedded"]["terms"]:
                    children.append(term["iri"])

        logger.debug(
            f"Found {len(children)} children for entity {entity_iri} from ontology {ontology}"
        )
        return children

    async def _findParent(self, ontology: str, entities: list[str]) -> str | None:
        """
        Finds the parent of a list of entities in the terminology service

        :param ontology str The ontology to search in
        :param entities list[str] The entities to search for

        :return str|None The parent entity of the entities
        """
        logger.debug(f"Finding parent of entities {entities} in ontology {ontology}")

        if len(entities) == 0:
            logger.error(f"No entities to search for in ontology {ontology}")
            return None

        # Get the children of each entity
        children = {}
        for entity in entities:
            children[entity] = await self._getChildren(ontology, entity)

        # Check if one of the entities is the parent of one of the others
        for entity in entities:
            for child in children[entity]:
                if child in entities:
                    logger.debug(f"Found parent {entity} of child {child}")
                    return entity
        logger.info(f"No parent found for entities {entities} in ontology {ontology}")

        # Check for entity with the most children
        max_children = 0
        parent = None
        for entity in entities:
            if len(children[entity]) > max_children:
                max_children = len(children[entity])
                parent = entity
        if parent is not None:
            logger.debug(f"Found {parent} with {max_children} children")
            return parent
        else:
            logger.error(
                f"No parent found for entities {entities} in ontology {ontology}"
            )
            return None

    @staticmethod
    def _validateCHEBI(node: dict) -> bool:
        """
        Validates a chemical entity node

        :param node:dict The node to validate (entity from the terminology service)

        :return:bool True if the node is a valid chemical entity, False otherwise
        """
        if "http://purl.obolibrary.org/obo/chebi/inchikey" in node:
            return True
        elif "http://purl.obolibrary.org/obo/chebi/smiles" in node:
            return True
        elif "http://purl.obolibrary.org/obo/chebi/inchi" in node:
            return True
        elif "http://purl.obolibrary.org/obo/chebi/mass" in node:
            return True
        elif "http://purl.obolibrary.org/obo/chebi/formula" in node:
            return True
        return False


if __name__ == "__main__":
    t = Terminology("https://api.terminology.tib.eu")

    logger.info(
        asyncio.run(
            t.searchForTerm("1H", "chebi", "http://purl.obolibrary.org/obo/CHEBI_33250")
        )
    )
    logger.info(
        asyncio.run(
            t.searchForTerm(
                "13C", "chebi", "http://purl.obolibrary.org/obo/CHEBI_33250"
            )
        )
    )
    logger.info("Searching for acetone")
    logger.info(
        asyncio.run(
            t.searchForTerm(
                "Acetone",
                "chebi",
                "http://purl.obolibrary.org/obo/CHEBI_46787",
            )
        )
    )
