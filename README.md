[
![Docs](https://img.shields.io/badge/read-docs-success)
](https://kit-data-manager.github.io/nmr_FAIR-DOs)
[
![CI](https://img.shields.io/github/actions/workflow/status/kit-data-manager/nmr_FAIR-DOs/ci.yml?branch=main&label=ci)
](https://github.com/kit-data-manager/nmr_FAIR-DOs/actions/workflows/ci.yml)


<!-- --8<-- [start:abstract] -->
# nmr_FAIR-DOs

This project creates FAIR Digital Objects (FAIR-DOs) for multiple repositories, registers them with the [Typed PID-Maker](https://github.com/kit-data-manager/pit-service) and indexes them in an Elasticsearch instance.

Currently, these repositories are supported:

- [NMRXiv](https://nmrxiv.org)
- [Chemotion](https://chemotion-repository.net)

See the [created FAIR-DOs](https://bwsyncandshare.kit.edu/s/P6qt5ecGsDa2yan) of the project.

If you want to explore these FAIR-DOs in a user-friendly manner, please visit the [search interface](https://metarepo.nffa.eu/nep-search).
For more information, see the [documentation](https://kit-data-manager.github.io/nmr_FAIR-DOs/main).


<!-- --8<-- [end:abstract] -->
<!-- --8<-- [start:quickstart] -->

## Installation

Clone this project and use [Poetry](https://python-poetry.org/) to install the dependencies.

```bash
git clone https://github.com/kit-data-manager/nmr_FAIR-DOs.git
cd nmr_FAIR-DOs
poetry install
```

This project works with Python > 3.8.

## Getting Started

Get started by running the command line interface (CLI) with the `nmr_FAIR-DOs` command. You can use the `--help` flag to see the available options.
```bash
poetry run nmr_FAIR-DOs-cli --help
```

To create FAIR-DOs for all NMR data in the repositories and log the output, run the following command:
```bash
poetry run nmr_FAIR-DOs-cli createallavailable 2>&1 | tee full.log
```

<!-- --8<-- [end:quickstart] -->

## Troubleshooting

### When I try installing the package, I get an `IndexError: list index out of range`

Make sure you have `pip` > 21.2 (see `pip --version`), older versions have a bug causing
this problem. If the installed version is older, you can upgrade it with
`pip install --upgrade pip` and then try again to install the package.

**You can find more information in the
[documentation](https://kit-data-manager.github.io/nmr_FAIR-DOs/main).**

<!-- --8<-- [start:citation] -->

## How to Cite

If you want to cite this project in your scientific work,
please use the [citation file](https://citation-file-format.github.io/)
in the [repository](https://github.com/kit-data-manager/nmr_FAIR-DOs/blob/main/CITATION.cff).

<!-- --8<-- [end:citation] -->
<!-- --8<-- [start:acknowledgements] -->

## Acknowledgements
This is a Python project generated from the [fair-python-cookiecutter](https://github.com/Materials-Data-Science-and-Informatics/fair-python-cookiecutter) template.

We kindly thank all
[authors and contributors](https://kit-data-manager.github.io/nmr_FAIR-DOs/latest/credits).

This tool was created at [Karlsruhe Institute of Technology (KIT)](https://kit.edu) at the [Scientific Computing Center (SCC)](https://scc.kit.edu) in the department [Data Exploitation Methods (DEM)](https://www.scc.kit.edu/ueberuns/dem.php).

This work is supported by the consortium NFDI-MatWerk, funded by the Deutsche Forschungsgemeinschaft (DFG, German Research Foundation) under the National Research Data Infrastructure – NFDI 38/1 – project number 460247524.
**TODO: relevant organizational acknowledgements (employers, funders)**

<!-- --8<-- [end:acknowledgements] -->
