# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the LICENSE
# file in the root directory of this source tree.

#!/usr/bin/env python3
# pyre-strict

import argparse
import subprocess
from time import sleep
from typing import List, Union

from windtunnel.cogwheel.helpers.smc import get_services_from_tier

from windtunnel.cogwheel.test import cogwheel_test, CogwheelTest

CONNECTION_PORT = 9999
FBPKG_NAME = "moxygen_test"


# Params For Client
class MoQClientParams:
    forwardingPreference: int = 0
    start_group: int = 0
    start_object: int = 0
    last_group: int = 3  # (2**62) - 1 ( Reduced For Testing Purposes )
    last_object: int = 10
    objects_per_group: int = 10
    size_of_object_zero: int = 1024
    size_of_object_greater_than_zero: int = 100
    object_frequency: int = 20
    group_increment: int = 1
    object_increment: int = 1
    test_integer_extension: int = -1
    test_variable_extension: int = -1
    send_end_of_group_markers: bool = False
    publisher_delivery_timeout: int = 0  # Not Implemented Yet
    request_type: str = "subscribe"  # subscribe or fetch


class MoqtestCogwheelTest(CogwheelTest):
    moqtest_server_tier: str = ""
    success_message = "MoQTest verification result: SUCCESS!"
    failure_message = "MoQTest verification result: FAILURE!"
    onObject_message = "MoQTest DEBUGGING: Calling onObject"
    onObjectStatus_message = "MoQTest DEBUGGING: calling onObjectStatus"

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

    def create_client_process_with_params(
        self, params: MoQClientParams, ipadd: str, port: int
    ) -> subprocess.Popen[bytes]:
        client_process = subprocess.Popen(
            [
                f"{self.get_package_path()}/{FBPKG_NAME}/client",
                "--logging",
                "DBG1",
                "--url",
                "https://[" + ipadd + "]:" + str(port),
                "--forwarding_preference",
                str(params.forwardingPreference),
                "--start_group",
                str(params.start_group),
                "--start_object",
                str(params.start_object),
                "--last_group",
                str(params.last_group),
                "--last_object_in_track",
                str(params.last_object),
                "--objects_per_group",
                str(params.objects_per_group),
                "--size_of_object_zero",
                str(params.size_of_object_zero),
                "--size_of_object_greater_than_zero",
                str(params.size_of_object_greater_than_zero),
                "--object_frequency",
                str(params.object_frequency),
                "--group_increment",
                str(params.group_increment),
                "--object_increment",
                str(params.object_increment),
                "--test_integer_extension",
                str(params.test_integer_extension),
                "--test_variable_extension",
                str(params.test_variable_extension),
                "--send_end_of_group_markers"
                if params.send_end_of_group_markers
                else "",
                "--publisher_delivery_timeout",
                str(params.publisher_delivery_timeout),
                "--request",
                str(params.request_type),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return client_process

    def wait_for_server_address(self) -> Union[None, str]:
        num_tries = 0
        while num_tries < 10:
            sleep(5)
            services = []
            try:
                services = get_services_from_tier(self.moqtest_server_tier)
                if len(services) > 0:
                    return services[0].ipaddr
            except Exception:
                pass
            num_tries += 1
        return None

    def get_client_output(self, params: MoQClientParams) -> List[str]:
        # Get the server address
        ip = self.wait_for_server_address()
        if not ip:
            self.fail("Failed to get server address")

        # Start the client
        client_process = self.create_client_process_with_params(
            params, ip, CONNECTION_PORT
        )

        # Wait for the client to finish
        client_process.wait()
        client_process_error = client_process.stderr
        if not client_process_error:
            self.fail("Client failed to connect to server error")
        error_lines = client_process_error.readlines()
        lines = []
        for line in error_lines:
            decoded_line = line.decode("utf8").strip()
            lines.append(decoded_line)
        return lines

    def check_lines(
        self,
        lines: List[str],
        test_name: str,
        forwarding_preference: Union[int, None] = None,
    ) -> bool:
        check = False
        for line in lines:
            if self.success_message in line:
                check = True
            if self.failure_message in line:
                if forwarding_preference:
                    self.fail(
                        test_name
                        + "ERROR: with forwarding preference "
                        + str(forwarding_preference)
                        + " resulted in error="
                        + line
                    )
                else:
                    self.fail(test_name + "ERROR: resulted in error=" + line)
        return check

    def check_lines_fetch(
        self,
        lines: List[str],
        test_name: str,
        forwarding_preference: Union[int, None] = None,
    ) -> bool:
        for line in lines:
            if self.failure_message in line:
                if forwarding_preference:
                    self.fail(
                        test_name
                        + "ERROR: with forwarding preference "
                        + str(forwarding_preference)
                        + " resulted in error="
                        + line
                    )
                else:
                    self.fail(test_name + "ERROR: resulted in error=" + line)
        return True

    @cogwheel_test
    def test_client_server_connection(self) -> None:
        """
        Test to verify that the client can connect to the server and recieves a Subscribe Success.
        """

        error_lines = self.get_client_output(MoQClientParams())
        check = self.check_lines(error_lines, "test_client_server_connection")

        self.assertTrue(check)
        self.cleanup()
        return None

    @cogwheel_test
    def test_subscribe_client_with_end_of_flag_markers(self) -> None:
        """
        Test To Verify End of Group Markers result in a success.
        Expect 10 onObject Calls Per Group and 1 onObjectStatus Call Per Group. (10 objects Per Group)
        Tested for first 3 forwarding preference types for subscriptions.
        """

        params = MoQClientParams()
        params.send_end_of_group_markers = True
        params.last_group = 3
        params.last_object = 10

        for i in range(0, 3):
            expected_objects = 40
            expected_end_of_group_markers = 4
            called_objects = 0
            called_objects_status = 0

            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)
            check = False

            for line in error_lines:
                if self.success_message in line:
                    check = True
                if self.onObject_message in line:
                    called_objects += 1
                if self.onObjectStatus_message in line:
                    called_objects_status += 1
                if self.failure_message in line:
                    self.fail(
                        "Subscribe with forwarding preference "
                        + str(i)
                        + " resulted in error="
                        + line
                    )
            # Check if Test Results in Success
            self.assertTrue(check)

            # Check if Object calls are correct
            self.assertEqual(called_objects, expected_objects)

            # Check if Object Status calls are correct
            self.assertEqual(called_objects_status, expected_end_of_group_markers)

        self.cleanup()
        return None

    @cogwheel_test
    def test_subscribe_client_with_extensions(self) -> None:
        """
        Test To Verify Extensions result in a success.
        Tested for all 4 forwarding preference types for subscriptions.
        """

        params = MoQClientParams()
        params.test_integer_extension = 10
        params.test_variable_extension = 10
        params.send_end_of_group_markers = False

        for i in range(0, 4):
            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)
            check = self.check_lines(
                error_lines, "test_subscribe_client_with_extensions", i
            )

            # Check if Test Results in Success
            self.assertTrue(check)
            # Check if Test Results in Success
            self.assertTrue(check)

        self.cleanup()
        return None

    @cogwheel_test
    def test_subscribe_start_group_equals_last_group(self) -> None:
        """
        Test To Verify that case of start_group = last_group results in a success.
        Tested For all 4 Forwarding Preference Types.
        """

        params = MoQClientParams()
        params.last_group = 0

        for i in range(0, 4):
            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)
            check = self.check_lines(
                error_lines, "test_subscribe_start_group_equals_last_group", i
            )
            # Check if Test Results in Success
            self.assertTrue(check)
        return None

    @cogwheel_test
    def test_subscribe_start_object_equals_last_object(self) -> None:
        """
        Test To Verify that case of start_object = last_object results in a success.
        Tested For all 4 Forwarding Preference Types.
        """

        params = MoQClientParams()
        params.last_object = 0
        params.objects_per_group = 1

        for i in range(0, 4):
            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)

            check = self.check_lines(
                error_lines, "test_subscribe_start_object_equals_last_object", i
            )
            # Check if Test Results in Success
            self.assertTrue(check)
        return None

    @cogwheel_test
    def test_fetch_client_server_connection(self) -> None:
        """
        Test to verify that the client can connect to the server using fetch and recieves a Success.
        """

        params = MoQClientParams()
        params.request_type = "fetch"
        error_lines = self.get_client_output(params)
        check = self.check_lines_fetch(
            error_lines, "test_fetch_client_server_connection"
        )

        self.assertTrue(check)
        self.cleanup()
        return None

    @cogwheel_test
    def test_fetch_client_with_end_of_flag_markers(self) -> None:
        """
        Test To Verify End of Group Markers result in a success.
        Expect 10 onObject Calls Per Group and 1 onObjectStatus Call Per Group. (10 objects Per Group)
        Tested for first 3 forwarding preference types for fetch.
        """

        params = MoQClientParams()
        params.send_end_of_group_markers = True
        params.last_group = 3
        params.last_object = 10
        params.request_type = "fetch"

        for i in range(0, 3):
            expected_objects = 40
            expected_end_of_group_markers = 4
            called_objects = 0
            called_objects_status = 0

            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)

            for line in error_lines:
                if self.onObject_message in line:
                    called_objects += 1
                if self.onObjectStatus_message in line:
                    called_objects_status += 1
                if self.failure_message in line:
                    self.fail(
                        "Fetch with forwarding preference "
                        + str(i)
                        + " resulted in error="
                        + line
                    )

            # Check if Object calls are correct
            self.assertEqual(called_objects, expected_objects)

            # Check if Object Status calls are correct
            self.assertEqual(called_objects_status, expected_end_of_group_markers)

        self.cleanup()
        return None

    @cogwheel_test
    def test_fetch_client_with_extensions(self) -> None:
        """
        Test To Verify Extensions result in a success.
        Tested for first 3 forwarding preference types for fetch.
        """

        params = MoQClientParams()
        params.test_integer_extension = 10
        params.test_variable_extension = 10
        params.request_type = "fetch"

        for i in range(0, 3):
            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)
            check = self.check_lines_fetch(
                error_lines, "test_fetch_client_with_extensions", i
            )
            # Check if Test Results in Success
            self.assertTrue(check)

        self.cleanup()
        return None

    @cogwheel_test
    def test_fetch_start_group_equals_last_group(self) -> None:
        """
        Test To Verify that case of start_group = last_group results in a success.
        Tested For first 3 Forwarding Preference Types.
        """

        params = MoQClientParams()
        params.last_group = 0
        params.request_type = "fetch"

        for i in range(0, 3):
            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)
            check = self.check_lines_fetch(
                error_lines, "test_fetch_start_group_equals_last_group", i
            )
            # Check if Test Results in Success
            self.assertTrue(check)
        return None

    @cogwheel_test
    def test_fetch_start_object_equals_last_object(self) -> None:
        """
        Test To Verify that case of start_object = last_object results in a success.
        Tested For first 3 Forwarding Preference Types.
        """

        params = MoQClientParams()
        params.last_object = 0
        params.request_type = "fetch"

        for i in range(0, 3):
            # Test For each Forwarding Preference
            params.forwardingPreference = i

            error_lines = self.get_client_output(params)
            check = self.check_lines_fetch(
                error_lines, "test_fetch_start_object_equals_last_object", i
            )
            # Check if Test Results in Success
            self.assertTrue(check)
        return None


def main() -> None:
    MoqtestCogwheelTest().main()


if __name__ == "__main__":
    main()
