"""MoQ Test Client Python Bindings Type Stubs"""

# @generated
# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

from typing import Any


def get_supported_params() -> dict[str, Any]:
    """Get dict of supported params with their default values.

    Returns:
        Dict mapping parameter name to default value.
        Required params have None as default.

    Example:
        >>> params = get_supported_params()
        >>> params["connect_url"]  # None (required)
        >>> params["forward"]  # True (default)
    """
    ...


class MoQClientError(Exception):
    """Exception raised when MoQ client operation fails.

    The exception message is in "code: message" format.
    Parse with str(e).split(": ", 1) to extract code and message.
    """

    pass


class PyMoQTestClient:
    """Python wrapper for MoQ test client. Parameters match C++ FLAGS exactly."""

    def __init__(
        self,
        *,  # Force keyword-only arguments
        # Required (same as C++ required FLAGS)
        connect_url: str,
        track_namespace: str,
        track_name: str,
        # Optional - maps exactly to C++ FLAGS names and defaults
        track_namespace_delimiter: str = "/",
        sg: str = "",  # start group
        so: str = "",  # start object
        eg: str = "",  # end group
        connect_timeout: int = 1000,  # ms
        transaction_timeout: int = 120,  # sec
        quic_transport: bool = False,
        use_legacy_setup: bool = False,
        insecure: bool = False,
        forward: bool = True,
        delivery_timeout: int = 0,
        fetch: bool = False,
        jrfetch: bool = False,  # joining relative fetch
        jafetch: bool = False,  # joining absolute fetch
        join_start: int = 0,
        publish: bool = False,
        unsubscribe: bool = False,
        unsubscribe_time: int = 30,
        mlog_path: str = "",
    ) -> None:
        """
        Initialize MoQ test client with parameters matching C++ FLAGS.

        Args:
            connect_url: WebTransport/QUIC URL (C++ --connect_url)
            track_namespace: Track namespace (C++ --track_namespace)
            track_name: Track name (C++ --track_name)
            track_namespace_delimiter: Namespace delimiter (C++ --track_namespace_delimiter)
            sg: Start group, empty for largest (C++ --sg)
            so: Start object, defaults to 0 when sg is set (C++ --so)
            eg: End group (C++ --eg)
            connect_timeout: Connect timeout in ms (C++ --connect_timeout)
            transaction_timeout: Transaction timeout in seconds (C++ --transaction_timeout)
            quic_transport: Use raw QUIC transport (C++ --quic_transport)
            use_legacy_setup: Use legacy moq-00 ALPN (C++ --use_legacy_setup)
            insecure: Skip certificate validation (C++ --insecure)
            forward: Forward flag for subscriptions (C++ --forward)
            delivery_timeout: Delivery timeout in ms, 0=disabled (C++ --delivery_timeout)
            fetch: Use fetch rather than subscribe (C++ --fetch)
            jrfetch: Joining relative fetch (C++ --jrfetch)
            jafetch: Joining absolute fetch (C++ --jafetch)
            join_start: Join start position (C++ --join_start)
            publish: Act as receiver for publish (C++ --publish)
            unsubscribe: Unsubscribe after time (C++ --unsubscribe)
            unsubscribe_time: Time before unsubscribe in seconds (C++ --unsubscribe_time)
            mlog_path: Path for MoQ logs (C++ --mlog_path)

        Raises:
            ValueError: If required params missing or mutually exclusive params conflict
        """
        ...

    def run(self) -> None:
        """
        Run the MoQ client. Behavior determined by constructor options.

        - Default: Subscribe mode
        - fetch=True: Fetch mode
        - jafetch=True: Joining absolute fetch mode
        - jrfetch=True: Joining relative fetch mode
        - publish=True: Publish mode (SubscribeAnnounces)

        Raises:
            MoQClientError: If operation fails. Check code and message for details.
                - code=-1: Invalid URL
                - code=-2: Exception occurred
                - code=-3: Fetch failed
                - code=-4: Subscribe failed
                - code=-5: Stream error
                - code=-6: Other exception
                - code=-7: SubscribeAnnounces failed
        """
        ...
