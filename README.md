# vertx-redis-resp3-map-keys-decoding-bug
Reproduces the bug in `vertx-redis-client` where RESP3 parser does not handle "bulk strings" (binary data) properly in map keys.
