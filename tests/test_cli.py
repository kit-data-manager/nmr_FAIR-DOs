"""Tests for the CLI."""

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

from typer.testing import CliRunner

runner = CliRunner()

# def test_calc_addition():
#     result = runner.invoke(app, ["calc", "add", "20", "22"])
#
#     assert result.exit_code == 0
#     assert result.stdout.strip() == "Result: 42"
#
#
# person_names = ["Jane", "John"]
#
#
# @pytest.mark.parametrize(
#     "name, formal",
#     [(name, formal) for name in person_names for formal in [False, True]],
# )
# def test_goodbye(name: str, formal: bool):
#     args = ["say", "goodbye", name]
#     if formal:
#         args += ["--formal"]
#
#     result = runner.invoke(app, args)
#
#     assert result.exit_code == 0
#     assert name in result.stdout
#     if formal:
#         assert "good day" in result.stdout
#
#
# # Example of hypothesis auto-generated inputs,
# # here the names are generated from a regular expression.
#
# # NOTE: this is not really a good regex for names!
# person_name_regex = r"^[A-Z]\w+$"
#
#
# @given(st.from_regex(person_name_regex))
# def test_hello(name: str):
#     result = runner.invoke(app, ["say", "hello", name])
#
#     assert result.exit_code == 0
#     assert f"Hello {name}" in result.stdout
