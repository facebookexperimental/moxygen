# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
#!/usr/bin/env python3
# pyre-strict

import argparse
import subprocess
from time import sleep
from typing import Union

from windtunnel.cogwheel.helpers.smc import get_services_from_tier

from windtunnel.cogwheel.test import cogwheel_test, CogwheelTest

CONNECTION_PORT = 9999
FBPKG_NAME = "moxygen_test"


class MoqtestCogwheelTest(CogwheelTest):
    moqtest_server_tier: str = ""
    success_message = "MoQTest verification result: SUCCESS!"
    failure_message = "MoQTest verification result: FAILURE!"

    def __init__(self) -> None:
        super(MoqtestCogwheelTest, self).__init__()

    def setUp(self) -> None:
        super().setUp()
        self.moqtest_server_tier = self.args.moqtest_server_tier

    def cleanup(self) -> None:
        return None

    def add_custom_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--moqtest_server_tier",
            help="Tier containing the moqtest server set up by Cogwheel",
            required=True,
        )
        return None

    # Create a basic client process that connects given url
    def create_basic_client_process(
        self, ipadd: str, port: int
    ) -> subprocess.Popen[bytes]:
        print(
            "client attempting to connect to " + "https://[" + ipadd + "]:" + str(port)
        )
        client_process = subprocess.Popen(
            [
                f"{self.get_package_path()}/{FBPKG_NAME}/client",
                "--logging",
                "DBG1",
                "--url",
                "https://[" + ipadd + "]:" + str(port),
                "--last_group",
                "3",  # Default Value of 3
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return client_process

    def wait_for_server_address(self) -> Union[None, str]:
        num_tries = 0
        while num_tries < 5:
            sleep(5)
            services = get_services_from_tier(self.moqtest_server_tier)
            if len(services) > 0:
                return services[0].ipaddr
            num_tries += 1
        return None

    @cogwheel_test
    def test_client_server_connection(self) -> None:
        """
        Test to verify that the client can connect to the server and recieves a Subscribe Success.
        """

        # Get the server address
        ip = self.wait_for_server_address()
        if not ip:
            self.fail("Failed to get server address")

        # Start the client
        client_process = self.create_basic_client_process(ip, CONNECTION_PORT)

        # Wait for the client to finish
        client_process.wait()

        # Check the client process output (Logs found in stderr)
        client_process_error = client_process.stderr
        check = False

        if client_process_error:
            error_lines = client_process_error.readlines()
            for line in error_lines:
                decoded_line = line.decode("utf8").strip()
                if self.success_message in decoded_line:
                    check = True
                if self.failure_message in decoded_line:
                    self.fail(
                        "Client failed to connect to server error=" + decoded_line
                    )

        self.assertTrue(check)
        self.cleanup()
        return None


def main() -> None:
    MoqtestCogwheelTest().main()


if __name__ == "__main__":
    main()
