# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
# pyre-strict

"""Unit tests for PyMoQTestClient Python binding.

Tests verify that Python CLI parameters match C++ binding parameters.
"""

import unittest

from ti.experimental.moxygen.samples.text_client.moq_client_pybinding import (
    get_supported_params,
)
from ti.experimental.moxygen.samples.text_client.py_moq_client_tester import (
    main as cli_main,
)


class PyMoQClientParamTest(unittest.TestCase):
    """Test that Python CLI params match C++ binding params."""

    def test_cli_params_match_binding_params(self) -> None:
        """py_moq_client_tester CLI params should match get_supported_params().

        This ensures the CLI (click) and C++ binding stay in sync.
        If someone adds a param to C++ but forgets to add it to the CLI,
        this test will fail.
        """
        # Get CLI param names from click command
        cli_param_names = {p.name for p in cli_main.params if p.name != "help"}

        # Get binding param names from get_supported_params()
        binding_param_names = set(get_supported_params().keys())

        # Check for params in binding but not in CLI
        missing_in_cli = binding_param_names - cli_param_names
        self.assertEqual(
            missing_in_cli,
            set(),
            f"Params in C++ binding but missing in CLI: {missing_in_cli}. "
            "Add these to py_moq_client_tester.py",
        )

        # Check for params in CLI but not in binding
        extra_in_cli = cli_param_names - binding_param_names
        self.assertEqual(
            extra_in_cli,
            set(),
            f"Params in CLI but not in C++ binding: {extra_in_cli}. "
            "Remove these from py_moq_client_tester.py or add to C++ binding",
        )


if __name__ == "__main__":
    unittest.main()
