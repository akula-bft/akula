# Tasks

## Retry fetching the top partial slice

When the sync is caught up, the top slice is usually not full (partial, i.e. len < 192).
Retry fetching it to fill it more and more.
It needs to be retried a bit more than its natural 30 sec timeout.


## Forky: rollback retries

Assuming that the slices connect to FC or CC, the fork mode algorithm makes progress, because at each fork mode step the chains grow, get longer or reach a higher TD mark. If they don’t grow the algorithm stalls. 

A solution is to perform an incremental rollback: after retrying a limited number of times the respective chain is cut more and more, e.g.:

1. 2 retries fail - cut 1 slice;
1. 4 retries fail - cut 2 slices;
1. 6 retries fail - cut 4 slices;
1. etc.

This either leads to finding an alternative proper path in the tree, or rolls all the way back to the linear case to restart from the beginning of the fork mode if needed.


## Forky: partial top slice support

In the forky phase there's no "final" header, it continues forever following the tip.
The tip slice is usually _partial_, e.g. len < 192.

We need to ensure that all slices that we get have size exactly 192,
except this tip slice. 

Yet, we don't know exactly where this slice is located in the chain,
so we need to handle this dynamically,
and be prepared at any point in time, that the last slice can be partial.

These things need to be addressed:

* test what happens if given a partial slice
* ForkModeStage: slice size checks are missing - add them as appropriate
* VerifyLinkLinearStage::verify_slice_link: don't check the size for the last forky slice
* ForkModeStage/VerifyLinkLinearStage: refetch the partial slice if it gets a new slice on top


## Forky: load fork chain slices from DB

When the fork mode is started, currently the algorithm tries to refetch
the slices to connect FC to CC from the network.

This works fine as long as there are not so many forks,
but at the tip of the chain the frequency of forks increases,
and this becomes wasteful.

When the fork mode is started (ForkModeStage::setup),
we should try to fill the fork_header_slices from DB,
and check the termination conditions immediately.
If the chain connects, the fork mode can be terminated right away
(picking the total difficulty winner).

The fork headers are not deleted from the database,
so this is likely to happen. For example, on Ropsten with 2 competing forks,
this optimization should lead to a much faster correct CC switches.


## VerifyStageLinearLink DoS (rollback)

If a rogue node takes a good slice, replaces a canonical top block with an alternative valid top block,
and sends it to us, the downloader will be stuck forever:
it will kick all peers that provide a next slice, because it's gonna fail to link.
This situation also happens if we accidentally get a slice that forked.

Solution:
If we fail to link a particular slice 2nd time,
invalidate the parent slice too (penalize and re-request it).
If that slice was saved, we need to stop and unwind its blocks.

This also helps for restarting after a long inactivity period.
If we synced into forky stage, and then stopped the server for
a long period of time such that we're back into linear phase upon restart,
this invalidation rolls back to a better fork.

This is related to forky rollback retries.


## Handle new block announces

When the sync is caught up, following the CC tip is based on the some other P2P nodes to be in sync.
If the network has only Akula nodes and miners, CC won't grow, because the miners' announces won't be handled by any node.

Messages to handle:

* [NewBlock](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#newblock-0x07):
    Try to extend CC with this block.
* [NewBlockHashes](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#newblockhashes-0x01):
    This message contains just the new block numbers.
    Request the headers that we don't know about from the same peer we've got the hashes from,
    and then try to extend CC.


## SentryStatusProvider: change max_block to be the overall progress

Currently, max_block/best_hash are based on the tables::LastHeader value,
but this corresponds to the HeaderDownload stage progress,
not overall progress.

This needs to reflect the overall sync progress.
Need to use the dedicated stage progress DB tables.

