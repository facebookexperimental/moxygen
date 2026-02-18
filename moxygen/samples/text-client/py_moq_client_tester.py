#!/usr/bin/env python3
# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# pyre-strict

"""
MoQ Test Client - Python binding with parameters matching C++ FLAGS exactly.

Example usage (same as C++ client):
    buck2 run //ti/experimental/moxygen/samples/text-client:py_moq_client_tester -- \\
        --connect_url "https://[::1]:3434/moq" \\
        --track_namespace "/test/subscribe" \\
        --track_name "basic_track" \\
        --use_legacy_setup
"""

import sys

import click
from ti.experimental.moxygen.samples.text_client.moq_client_pybinding import (
    MoQClientError,
    PyMoQTestClient,
)


@click.command()
# Required - maps to C++ required FLAGS
@click.option(
    "--connect_url",
    "-u",
    required=True,
    help="WebTransport/QUIC URL (C++ --connect_url)",
)
@click.option(
    "--track_namespace",
    "-n",
    required=True,
    help="Track namespace (C++ --track_namespace)",
)
@click.option(
    "--track_name",
    "-t",
    required=True,
    help="Track name (C++ --track_name)",
)
# Optional - maps exactly to C++ FLAGS
@click.option(
    "--track_namespace_delimiter",
    default="/",
    help="Namespace delimiter (C++ --track_namespace_delimiter)",
)
@click.option(
    "--sg",
    default="",
    help="Start group, empty for largest (C++ --sg)",
)
@click.option(
    "--so",
    default="",
    help="Start object (C++ --so)",
)
@click.option(
    "--eg",
    default="",
    help="End group (C++ --eg)",
)
@click.option(
    "--connect_timeout",
    type=int,
    default=1000,
    help="Connect timeout in ms (C++ --connect_timeout)",
)
@click.option(
    "--transaction_timeout",
    type=int,
    default=120,
    help="Transaction timeout in seconds (C++ --transaction_timeout)",
)
@click.option(
    "--quic_transport/--no-quic_transport",
    default=False,
    help="Use raw QUIC transport (C++ --quic_transport)",
)
@click.option(
    "--use_legacy_setup/--no-use_legacy_setup",
    default=False,
    help="Use legacy moq-00 ALPN (C++ --use_legacy_setup)",
)
@click.option(
    "--insecure/--no-insecure",
    default=False,
    help="Skip certificate validation (C++ --insecure)",
)
@click.option(
    "--forward/--no-forward",
    default=True,
    help="Forward flag for subscriptions (C++ --forward)",
)
@click.option(
    "--delivery_timeout",
    type=int,
    default=0,
    help="Delivery timeout in ms, 0=disabled (C++ --delivery_timeout)",
)
@click.option(
    "--fetch/--no-fetch",
    default=False,
    help="Use fetch rather than subscribe (C++ --fetch)",
)
@click.option(
    "--jrfetch/--no-jrfetch",
    default=False,
    help="Joining relative fetch (C++ --jrfetch)",
)
@click.option(
    "--jafetch/--no-jafetch",
    default=False,
    help="Joining absolute fetch (C++ --jafetch)",
)
@click.option(
    "--join_start",
    type=int,
    default=0,
    help="Join start position (C++ --join_start)",
)
@click.option(
    "--publish/--no-publish",
    default=False,
    help="Publish mode - SubscribeAnnounces (C++ --publish)",
)
@click.option(
    "--unsubscribe/--no-unsubscribe",
    default=False,
    help="Unsubscribe after time (C++ --unsubscribe)",
)
@click.option(
    "--unsubscribe_time",
    type=int,
    default=30,
    help="Time before unsubscribe in seconds (C++ --unsubscribe_time)",
)
@click.option(
    "--mlog_path",
    default="",
    help="Path for MoQ logs (C++ --mlog_path)",
)
def main(
    connect_url: str,
    track_namespace: str,
    track_name: str,
    track_namespace_delimiter: str,
    sg: str,
    so: str,
    eg: str,
    connect_timeout: int,
    transaction_timeout: int,
    quic_transport: bool,
    use_legacy_setup: bool,
    insecure: bool,
    forward: bool,
    delivery_timeout: int,
    fetch: bool,
    jrfetch: bool,
    jafetch: bool,
    join_start: int,
    publish: bool,
    unsubscribe: bool,
    unsubscribe_time: int,
    mlog_path: str,
) -> None:
    """MoQ Test Client - Python binding with parameters matching C++ FLAGS exactly."""
    # Determine mode for display
    if publish:
        mode = "publish"
    elif fetch:
        mode = "fetch"
    elif jafetch:
        mode = "jafetch"
    elif jrfetch:
        mode = "jrfetch"
    else:
        mode = "subscribe"

    # Print configuration (matching C++ output style)
    print("=" * 60)
    print("MoQ Test Client (Python Binding)")
    print("=" * 60)
    print(f"URL: {connect_url}")
    print(f"Track Namespace: {track_namespace}")
    print(f"Track Name: {track_name}")
    print(f"Mode: {mode}")
    print(f"QUIC Transport: {quic_transport}")
    print(f"Legacy Setup: {use_legacy_setup}")
    print(f"Insecure: {insecure}")
    print(f"Connect Timeout: {connect_timeout}ms")
    print(f"Transaction Timeout: {transaction_timeout}s")
    if sg:
        print(f"Start Group: {sg}")
    if so:
        print(f"Start Object: {so}")
    if eg:
        print(f"End Group: {eg}")
    print("=" * 60)

    try:
        client = PyMoQTestClient(
            connect_url=connect_url,
            track_namespace=track_namespace,
            track_name=track_name,
            track_namespace_delimiter=track_namespace_delimiter,
            sg=sg,
            so=so,
            eg=eg,
            connect_timeout=connect_timeout,
            transaction_timeout=transaction_timeout,
            quic_transport=quic_transport,
            use_legacy_setup=use_legacy_setup,
            insecure=insecure,
            forward=forward,
            delivery_timeout=delivery_timeout,
            fetch=fetch,
            jrfetch=jrfetch,
            jafetch=jafetch,
            join_start=join_start,
            publish=publish,
            unsubscribe=unsubscribe,
            unsubscribe_time=unsubscribe_time,
            mlog_path=mlog_path,
        )
        client.run()
        print("\n" + "=" * 60)
        print("Test PASSED")
        print("=" * 60)
    except MoQClientError as e:
        # Exception message is "code: message" format
        error_str = str(e)
        if ": " in error_str:
            code_str, msg = error_str.split(": ", 1)
            print(f"\n{'=' * 60}")
            print(f"Test FAILED (code={code_str})")
            print(f"Error: {msg}")
        else:
            print(f"\n{'=' * 60}")
            print(f"Test FAILED: {error_str}")
        print("=" * 60)
        sys.exit(1)
    except Exception as e:
        print(f"\n{'=' * 60}")
        print(f"Unexpected error: {e}")
        print("=" * 60)
        sys.exit(2)


if __name__ == "__main__":
    main()
