# Header downloader

The header downloader module is responsible for syncing the canonical blockchain headers
database with the P2P network state (using a sentry client).
It can start from scratch to download and assemble the canonical chain of headers.
When synced, it updates the canonical chain following the tip and handling possible fork switches.

* [Specification](spec.md)
* [Testing](testing.md)
* [TODO](TODO.md)
