"""MoQ Simple Publisher Python Bindings Type Stubs"""

# @generated
# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.


class MoQPublisherError(Exception):
    """Exception raised when MoQ publisher operation fails.

    The exception message is in "code: message" format.
    Parse with str(e).split(": ", 1) to extract code and message.
    """

    pass


class PyMoQSimplePublisher:
    """Python wrapper for MoQ simple publisher.

    This publisher connects to a MoQ server, announces a track namespace,
    waits for a subscriber, and sends messages to that subscriber.
    """

    def __init__(
        self,
        *,  # Force keyword-only arguments
        # Required
        connect_url: str,
        track_namespace: str,
        track_name: str,
        # Optional
        track_namespace_delimiter: str = "/",
        message: str = "hello",
        count: int = 1,
        interval_ms: int = 100,
        connect_timeout: int = 1000,  # ms
        transaction_timeout: int = 120,  # sec
        quic_transport: bool = False,
        use_legacy_setup: bool = False,
        insecure: bool = False,
    ) -> None:
        """
        Initialize MoQ simple publisher.

        Args:
            connect_url: WebTransport/QUIC URL (e.g., "https://[::1]:3491/moq")
            track_namespace: Track namespace (e.g., "/test/echo")
            track_name: Track name (e.g., "echo0")
            track_namespace_delimiter: Namespace delimiter (default: "/")
            message: Message content to send (default: "hello")
            count: Number of messages to send (default: 1)
            interval_ms: Interval between messages in ms (default: 100)
            connect_timeout: Connect timeout in ms (default: 1000)
            transaction_timeout: Transaction timeout in seconds (default: 120)
            quic_transport: Use raw QUIC transport instead of WebTransport (default: False)
            use_legacy_setup: Use legacy moq-00 ALPN (default: False)
            insecure: Skip certificate validation (default: False)

        Raises:
            ValueError: If required params are missing
        """
        ...

    def run(self) -> None:
        """
        Run the publisher.

        This method:
        1. Connects to the server
        2. Announces the track namespace
        3. Waits for a subscriber to connect
        4. Sends the specified number of messages
        5. Sends END_OF_TRACK and closes the connection

        Raises:
            MoQPublisherError: If operation fails. Check code and message for details.
                - code=-1: Invalid URL
                - code=-2: Exception occurred
                - code=-3: PUBLISH failed
                - code=-4: PUBLISH_OK error (timeout or rejection)
                - code=-5: objectStream failed
                - code=-6: Other exception
        """
        ...
