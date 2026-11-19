# Pastry in Erlang

An implementation of the Pastry overlay network (Rowstron & Druschel, 2001),
using the actor model, plus a simple key → string storage service built on
top of it.

## Team members

**Group 6**

- Sreenidhee Vidya Modugu (UFID: 7634 8994)
- Manvitha Gangasani (UFID: 3747 1957)

## How to run

Requirements:
- Erlang/OTP with `erl` and `erlc` on the PATH. Tested on OTP 29.
- `make` is optional. The `./project3` launcher compiles `src/` into `ebin/`
  when needed, using `make` if it is installed and `erlc` directly if not.

```
./project3 numNodes numRequests
```

Example:

```
$ ./project3 1000 10
Pastry: 1000 nodes, 10 requests per node
Network built in 0.30 s (average join path 2.13 hops)

Requests finished in 10.0 s
Requests sent: 10000, answered: 10000, timed out: 0
Stores: 5000, lookups: 5000 (correct 5000, wrong 0, not found 0)
Expected hops (log16 N): 2.491
Average number of hops: 2.488
```

The last line is the required output: the average number of hops (node to
node transfers) needed to deliver a request to the node responsible for its
key. The other lines are progress and checks. To print only the hop line,
use `-q`:

```
$ ./project3 -q 1000 10
Average number of hops: 2.493
```

Optional failure-model arguments (bonus, see `REPORT.md`):

```
./project3 numNodes numRequests [failPct [linkFailPct [dropPct]]]
```

- `numNodes` and `numRequests` must be at least 1.
- `failPct` and `linkFailPct` must be between 0 and 100.
- `dropPct` must be at least 0 and below 100.

The program prints a usage message if any of these is out of range.

**Results vary from run to run.** Node ids are fixed (the MD5 hash of the
node's index), but node coordinates, request keys and the choice of failed
nodes and links are random. Hop averages are stable to about ±0.02.

## What is working

All of the required parts work.

- **One actor per peer.** Each peer is its own Erlang process
  (`pastry_node`). Peers and the driver communicate only by sending
  messages.
- **Pastry state**, as in the paper. Ids are 128 bits with b = 4, so they
  are 32 hex digits. Each node keeps:
  - a routing table with up to 32 rows of 16 entries;
  - a leaf set with |L| = 16 (8 smaller, 8 larger);
  - a neighbourhood set with |M| = 16.

  Each node also gets a random 2-D coordinate, which stands in for network
  distance (the paper's proximity metric). When two nodes compete for the
  same routing-table slot, the closer one is kept.
- **Join (section 2.4).** A new node X asks a nearby node A to route a
  `join` message with key X.
  - Nodes on the path send X routing-table rows.
  - A also sends its neighbourhood set.
  - The last node Z (numerically closest to X) sends its leaf set.

  X builds its state from this, then sends its own state to every node it
  now knows, and those nodes update their tables.
- **Routing (section 2.2):**
  - If the key falls inside the leaf set range, deliver to the numerically
    closest leaf.
  - Otherwise use routing-table entry `R[l][D_l]`.
  - Otherwise ("rare case"), forward to any known node that shares at least
    as long a prefix with the key and is numerically closer to it.
- **API (section 2.3).**
  - Pastry exposes `route(msg, key)`:
    - `pastry_node:route/2` is called by the application inside a node;
    - `pastry_node:route/3` asks a given node to route a message, from any
      process.
  - The application implements the callbacks `deliver(msg, key)`,
    `forward(msg, key, nextId)` and `newLeafs(leafSet)` (`kv_app`).
- **Application.** `kv_app` maps a key (in the same 128-bit space as node
  ids) to a string. Each peer sends one request per second:
  - odd-numbered requests store a new (key, string) pair;
  - even-numbered requests read back a key the peer stored earlier and
    check that the string is the right one.

  A lookup waits until the peer's first store has been confirmed, so every
  peer does exactly ⌈n/2⌉ stores and ⌊n/2⌋ lookups. The node responsible for
  the key replies with the hop count. When every peer has finished
  `numRequests` requests, the program prints the average hop count and
  exits.
- **No fixed time limit.** The driver waits as long as the requests take
  (it gives up only after `numRequests + 60` seconds of silence), so large
  `numRequests` such as `./project3 100 100` work.
- **Correctness check.** `test_routing` routes random keys from random nodes
  and checks that each key reaches the live node whose id is numerically
  closest to it. Every probe passed in every run without failures (10, 17
  and 2000 nodes).

  ```
  make
  erl -noshell -pa ebin -run test_routing main 2000 2000
  ```

Measured average hops follow the expected log₁₆ N growth (3 requests per
node):

| Nodes   | Avg hops | log₁₆ N |
|--------:|---------:|--------:|
| 100     | 1.72     | 1.66    |
| 1,000   | 2.50     | 2.49    |
| 10,000  | 3.24     | 3.32    |
| 50,000  | 3.76     | 3.90    |
| 100,000 | 3.98     | 4.15    |
| 200,000 | 4.27     | 4.40    |

### Design choices and deviations from the paper

- **Join rows.** In the paper, the i-th node on the join path gives X its
  row i. Here, each node on the path gives X its rows 0..i, where i is the
  length of the prefix it shares with X. Every one of those entries is valid
  for X, so this is a superset of the paper's rows. X's table starts at
  least as full, at the cost of larger join messages.
- **Sequential joins.** The driver adds nodes one at a time and waits for
  each join to finish (X has heard back from every node it announced itself
  to) before starting the next. The paper also handles concurrent joins;
  that case is not implemented or tested here.
- **Bootstrap node.** The paper assumes a new node knows a nearby Pastry
  node. The driver approximates this by picking the closest of 8 random
  existing nodes.
- **Application inside the node.** The application (`kv_app`) is a library
  that runs inside each node process, like an application layered on a
  Pastry node. It is not a separate actor. This is why `route/2` works only
  from inside a node; `route/3` can be used from anywhere.

## Largest network

**200,000 nodes** (`./project3 200000 3`) on a MacBook Air with 8 cores and
8 GB of RAM:

- building the network took 159 s;
- all 600,000 requests were answered;
- all 200,000 lookups returned the correct string;
- the average was 4.27 hops;
- peak memory use was about 2.5 GB.

At this size the machine can't keep up with 200,000 requests per second, so
the request phase took 50 s instead of about 3 s. Up to 100,000 nodes, the
one-request-per-second rate holds (3 requests finished in about 8.5 s).

Memory is the limit on this machine. Two things keep it down:

- Idle nodes hibernate, which shrinks their heap to the data they hold
  (about 10 KB per node).
- Nodes are spawned with `fullsweep_after = 0`, so garbage does not pile up.

## Files

| File | Purpose |
|------|---------|
| `project3`            | Launcher script (compiles if needed, then runs) |
| `src/project3.erl`    | Driver: builds the network, starts the requests, collects results, injects failures |
| `src/pastry_node.erl` | The Pastry node actor: state, join, routing, simulated links, failure repair |
| `src/kv_app.erl`      | Key → string service on top of Pastry |
| `src/test_routing.erl`| Checks that keys reach the numerically closest node |
| `REPORT.md`           | Bonus: failure models, how they were tested, results |
