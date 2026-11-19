# Bonus report – failures in Pastry

## Failure models

The optional arguments switch on three kinds of failure:

```
./project3 numNodes numRequests failPct linkFailPct dropPct
```

| Argument      | Failure | When |
|---------------|---------|------|
| `failPct`     | **Node dies.** That percentage of nodes is killed (`exit(Pid, kill)`). | Once, halfway through the requests |
| `linkFailPct` | **Connection dies permanently.** Each surviving node picks that percentage of its links. Both ends stop using them. | Same moment |
| `dropPct`     | **Connection fails temporarily.** Every single transmission between two nodes is lost with this probability. | From the start of the requests to the end of the run |

Failures happen while requests are in flight. About half of the lookups
then ask for values stored before the failure, which tests whether stored
data survives, not just whether routing still works.

### What the link failures apply to

When any failure argument is given, every message between two nodes during
the request phase goes through one function, `transmit/3`:

- a message on a broken link is always lost;
- any other message is lost with probability `dropPct`;
- lost messages really are lost: they are never delivered.

This covers:
- routing hops;
- hop acknowledgements;
- replies to requests, which are routed back to the requester through
  Pastry;
- replica copies;
- leaf-set and routing-table repair messages.

Some messages are not subject to link failures:

- **Join and announce messages.** The network is fully built before any
  failure starts.
- **Simulation control messages** from the driver: start, kill, break links,
  collect statistics.

## How the system handles failures

**Reliable hops (acks, timeouts, resends).**
- In failure mode, each message sent from one node to another carries a tag,
  and the receiver acknowledges it.
- If no ack arrives in time, the sender resends the message. The wait is
  100 ms, then 200 ms, then 400 ms (3 transmissions in total).
- The receiver drops resent copies it has already seen, so a lost ack never
  causes a message to be handled twice.
- If all 3 transmissions go unacknowledged, the sender treats the link as
  down:
  - a routed message takes another route;
  - any other message is given up and counted.

Losses therefore cost real time: each lost transmission delays a message by
at least 100 ms.

**Detecting dead nodes.** Each node watches its peers with Erlang process
monitors (`erlang:monitor`). When a monitored peer dies, the runtime sends
the node a `'DOWN'` message immediately. This is not a heartbeat: inside one
Erlang VM, detection is instant and never wrong, which is more optimistic
than a real network. Monitors are only set up in failure mode, because
watching about 100 peers per node costs memory.

When a node learns a peer has died, messages still waiting for that peer's
ack are rerouted (if they are routed messages) or given up straight away.

**Repairing the leaf set (paper section 2.4).**

1. The node removes the dead peer.
2. It refills the leaf set in one pass from every node it still knows.
3. It asks the farthest live leaf on each side for that leaf's leaf set, and
   merges in the reply.

**Repairing the routing table (paper section 2.4).** The node removes the
dead entry `R[l][d]`. It then asks one random other entry in row `l` for its
own `R[l][d]`.

**Routing around broken links.**
- A routing-table hop that can't be used falls back to the paper's rare
  case: any node that is closer to the key and shares at least as long a
  prefix.
- If the destination is a leaf that can't be reached directly, the message
  is detoured through the closest reachable leaf, which can usually reach
  it.
- A detoured message records which nodes detoured it, so it can't bounce
  back and forth between two nodes that both lost their link to the
  destination.
- If no usable node is left, the message is **dropped and counted as "no
  route"**. It is never delivered at the wrong node. The request then times
  out.
- There is also a hard limit of 64 hops.

**Keeping data alive (replication, as in PAST).**
- The node responsible for a key stores the value. It also copies it to the
  next 4 nodes closest to the key (5 copies in total).
- When a node's leaf set changes, the `newLeafs` upcall copies each value it
  holds to any node that has just become one of that key's 5 closest.

**Request timeouts.** A request with no answer after 5 s counts as timed
out.

## How it was tested

1. **Routing correctness after failures.** `test_routing` builds a network,
   kills a share of the nodes, waits 2 s for repair, then routes random keys
   from random live nodes. For each key it checks that the node that
   delivered it is the live node numerically closest to the key.
2. **End-to-end workload.** `project3` runs with the failure arguments. For
   requests and lookups it prints:
   - requests answered and timed out;
   - lookups that returned the correct string, a wrong one, or nothing;
   - average hops.

   For the link layer it prints:
   - transmissions lost;
   - resends;
   - reroutes after a dead link;
   - messages dropped (no route, hop limit, direct sends given up).
3. **Fault-free baseline.** All runs without failures answer 100% of
   requests, and all lookups are correct.

## Results

### Routing correctness (`test_routing`, probes reaching the correct node)

| Nodes  | Killed | Correct |
|-------:|-------:|--------:|
| 2,000  | 0%     | 2000 / 2000 |
| 2,000  | 10%    | 2000 / 2000 |
| 2,000  | 20%    | 2000 / 2000 |
| 2,000  | 30%    | 2000 / 2000 |
| 10,000 | 50%    | 4994 / 5000 |

### Workload results

How to read the tables:
- "Killed" is `failPct`, "Broken" is `linkFailPct`, and "Lost" is
  `dropPct`.
- **Requests sent = answered + timed out.**
- "Lookups" counts lookups that got an answer, and **lookups = correct +
  not found**. No lookup ever returned a wrong string.

**2,000 nodes, 10 requests per node**

| Killed | Broken | Lost | Sent | Answered | Timed out | Lookups | Correct | Not found | Avg hops |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0%  | 0%  | 0%  | 20,000 | 20,000 | 0  | 10,000 | 10,000 | 0  | 2.71 |
| 30% | 0%  | 0%  | 14,000 | 14,000 | 0  | 7,000  | 6,999  | 1  | 2.72 |
| 50% | 0%  | 0%  | 10,000 | 10,000 | 0  | 5,000  | 4,967  | 33 | 2.70 |
| 0%  | 10% | 0%  | 20,000 | 20,000 | 0  | 10,000 | 10,000 | 0  | 2.85 |
| 0%  | 30% | 0%  | 20,000 | 19,962 | 38 | 9,983  | 9,983  | 0  | 3.42 |
| 0%  | 0%  | 10% | 20,000 | 20,000 | 0  | 10,000 | 10,000 | 0  | 2.70 |
| 0%  | 0%  | 30% | 20,000 | 20,000 | 0  | 10,000 | 10,000 | 0  | 2.77 |
| 20% | 10% | 10% | 16,000 | 15,979 | 21 | 7,980  | 7,980  | 0  | 2.87 |

**10,000 nodes, 10 requests per node**

| Killed | Broken | Lost | Sent | Answered | Timed out | Lookups | Correct | Not found | Avg hops |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10% | 0%  | 0%  | 90,000 | 90,000 | 0   | 45,000 | 45,000 | 0   | 3.24 |
| 30% | 0%  | 0%  | 70,000 | 69,999 | 1   | 34,999 | 34,967 | 32  | 3.29 |
| 50% | 0%  | 0%  | 50,000 | 49,986 | 14  | 24,994 | 24,619 | 375 | 3.33 |
| 20% | 20% | 20% | 80,000 | 79,785 | 215 | 39,811 | 39,776 | 35  | 3.72 |

**Link-layer counters for selected runs**

| Run (nodes / killed / broken / lost) | Lost | Resends | Reroutes | No route | Gave up |
|---|---:|---:|---:|---:|---:|
| 2,000 / 0 / 0 / 10%    | 35,100  | 34,058  | 786    | 0  | 255    |
| 2,000 / 0 / 0 / 30%    | 224,684 | 191,322 | 25,344 | 0  | 7,570  |
| 2,000 / 0 / 30% / 0    | 24,699  | 16,466  | 3      | 38 | 8,230  |
| 2,000 / 30% / 0 / 0    | 0       | 17,409  | 3      | 0  | 4,495  |
| 10,000 / 20% / 20% / 20% | 845,413 | 815,847 | 42,806 | 13 | 95,247 |

"Lost" counts transmissions lost on links, "Resends" counts transmissions
repeated after an ack timeout, and "Gave up" counts direct sends abandoned
after 3 unacknowledged transmissions.

## Findings

- **Routing survives node failures.** With half the nodes killed at once,
  99.9% of probes still reached the correct node. The hop count didn't grow,
  because the network got smaller.
- **Data loss follows the replication factor.** A value is lost only when
  all 5 of its copies die. With a fraction f of nodes killed, that happens
  with probability about f⁵. About half of the lookups target values stored
  before the failure, so the expected "not found" rate is about f⁵ / 2:

  | Killed | Expected (f⁵ / 2) | Measured (2k nodes) | Measured (10k nodes) |
  |-------:|------------------:|--------------------:|---------------------:|
  | 30%    | 0.12%             | 0.01%               | 0.09%                |
  | 50%    | 1.6%              | 0.7%                | 1.5%                 |

  Replication only explains the "not found" results. Messages are never
  delivered at a wrong node: if no route is left, the message is dropped
  and counted as "no route".
- **Where timeouts come from.**
  - *Link failures:* messages with no usable route. In the 30%-broken-links
    run, there were exactly 38 "no route" drops and 38 timeouts.
  - *Node failures:* a message is lost if a node acknowledged it and was
    then killed before forwarding it, because the resend state dies with
    that node. This affected at most 0.03% of requests.
  - *Combined failures at 10k nodes:* 0.27% of requests timed out, from both
    causes plus resend delays piling up.
- **Temporary link failures are absorbed by resending.** At a 30% chance of
  losing each transmission, every request was still answered. The cost is
  real latency: each lost transmission waits for an ack timeout of at least
  100 ms. The 10-request run took 11.8 s instead of 10.0 s. Hop counts barely
  change (2.77 vs 2.71), because reroutes only happen when a link fails 3
  times in a row.
- **Permanently broken links cost extra hops.** With 30% of links broken,
  the average path grew from 2.71 to 3.42 hops, because of detours and
  rare-case hops. 0.19% of requests had no route left. Replica copies across
  a broken link fail (counted as "gave up"), so some values have fewer than
  5 copies.
- **A mass failure causes a repair storm.**
  - Right after many nodes die together, every survivor repairs its tables
    at the same moment. That burst delays acks past the 100 ms timeout. This
    is why kill-only runs show thousands of resends even though no
    transmission was lost.
  - The resends are harmless, because duplicate copies are filtered.
  - Two changes kept the storm manageable:
    - asking one peer per lost routing-table entry instead of three (as the
      paper does);
    - letting nodes in failure mode stay awake 500 ms before hibernating.

    Together they cut timeouts in the 10k / 30%-killed run from 784 to 1.
- **Limitations.**
  - Failure detection uses Erlang process monitors, which are instant and
    perfect. A real system would use keep-alives and could suspect a live
    node by mistake.
  - Both ends of a broken link learn about it immediately (simulated). A
    real system would find out through ack timeouts.
  - Joins are sequential and happen before any failure, so joins running at
    the same time as failures were not tested.
  - When more than 8 consecutive nodes on one side of a leaf set die
    together, the "ask the farthest leaf" repair can need several rounds.
    That explains the 6 wrong probes at 50% failures.
  - Ack timeouts are fixed (with doubling), not adapted to measured round
    trip times.
