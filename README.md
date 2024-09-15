# Kafka Go

Building a simple Kafka server with Go (v1.22) step by step, following the
[instructions on codecrafters.io](https://codecrafters.io/challenges/kafka).

In this challenge, we build a toy Kafka clone that's capable of accepting and
responding to APIVersions & Fetch API requests. Along the way, we also learn about
encoding and decoding messages using the Kafka wire protocol. We also learn
about handling the network protocol, event loops, TCP sockets and more.

## How to try this out

1. Ensure you have `go (1.22)` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `app/server.go`.
