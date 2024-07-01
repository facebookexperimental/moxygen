# moxygen

This is an experimental media MOQ relay (AKA: CDN node) based on [MOQT](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/). It can be used in conjunction with following live encoder and player [moq-encoder-player](https://github.com/facebookexperimental/moq-encoder-player). Both repos allows us create a live streaming platform where we can control latency and quality (and others), so we can test scenarios from ultra low latency live (video call) to high quality (and high scale) live.

![moq-basic-block-diagram](./pics/basic_block_diagram.png)
Fig1: Basic block diagram

In the following figure you can see an overview of the relay architecture

TODO
Fig2: Relay architecture overview

## Installation

- Just execute (probably you will need `sudo` unless you run it like `root`):
```
./build.sh
```

## Test relay
- Generate self signed certificate
```
cd scripts
./create-server-certs.sh
```

### Test with date server

- Execute date server (from project root dir)
```
./_build/bin/moqdateserver -port 4433 -cert ./certs/certificate.pem -key ./certs/certificate.key --logging DBG
```

- Execute text client
```
./_build/bin/moqtextclient --connect_url "https://localhost:4433/moq-date" --track_namespace "moq-date" --track_name "/date"
```

- You should see an output like:
```
E0520 13:08:07.722896 7064889 MoQTextClient.cpp:148] Invalid url: localhost:4433/moq-date
I0520 13:08:07.723534 7064889 MoQTextClient.cpp:27] run
I0520 13:08:07.723554 7064889 MoQClient.cpp:31] setupMoQSession
I0520 13:08:07.729465 7064889 MoQClient.cpp:16] connectSuccess
I0520 13:08:07.731674 7064889 MoQClient.cpp:33] exit setupMoQSession
I0520 13:08:07.731702 7064889 MoQTextClient.cpp:94] controlReadLoop
I0520 13:08:07.731714 7064889 MoQSession.h:57] ServerSetup, version=4278190082
I0520 13:08:07.733063 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733076 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733080 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733082 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733084 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733086 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733089 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733091 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733094 7064889 MoQClient.cpp:137] onWebTransportUniStream
I0520 13:08:07.733716 7064889 MoQTextClient.cpp:107] readTrack
7
6
5
4
3
2
1
0
2024-05-20 13:08:
I0520 13:08:08.336642 7064889 MoQClient.cpp:137] onWebTransportUniStream
8
I0520 13:08:09.344473 7064889 MoQClient.cpp:137] onWebTransportUniStream
9
I0520 13:08:10.346489 7064889 MoQClient.cpp:137] onWebTransportUniStream
10
I0520 13:08:11.351163 7064889 MoQClient.cpp:137] onWebTransportUniStream
11
```

## Test with media client (from MACOS)

- Execute (from project root dir)
```
./_build/bin/moqrelayserver -port 4433 -cert ./certs/certificate.pem -key ./certs/certificate.key -endpoint "/moq" --logging DBG
```
- Start client (MACOS)
```
cd scripts
./macos-start-localhost-test-chrome.sh
```
- Run MOQ encoder locally
   - From chrome started before, run this project [moq-encoder-player](https://github.com/facebookexperimental/moq-encoder-player)


## License

moxygen is released under the [MIT License](https://github.com/facebookexperimental/moqxygen/blob/main/LICENSE).
