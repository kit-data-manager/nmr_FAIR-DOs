"""Test for core library."""

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

#
# def test_calculate_invalid():
#     with pytest.raises(ZeroDivisionError):
#         calculate(CalcOperation.divide, 123, 0)
#
#     with pytest.raises(ValueError):
#         calculate("invalid", 123, 0)  # type: ignore
#
#     with pytest.raises(NotImplementedError):
#         calculate(CalcOperation.power, 2, 3)
#
#
# # Example of how hypothesis can be used to generate different
# # combinations of inputs automatically:
#
#
# @given(st.sampled_from(CalcOperation), st.integers(), st.integers())
# def test_calculate(op, x, y):
#     # assume can be used to ad-hoc filter outputs -
#     # if the assumption is violated, the test instance is skipped.
#     # (better: use strategy combinators for filtering)
#     assume(op != CalcOperation.divide or y != 0)
#     assume(op != CalcOperation.power)  # not supported
#
#     # we basically just check that there is no exception and the type is right
#     result = calculate(op, x, y)
#     assert isinstance(result, int)
