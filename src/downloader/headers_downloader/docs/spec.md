# Header downloader spec

The download process has 3 phases: preverified, linear and forky.

    block numbers = [0, 1, 2, …, PN, PN + 1, PN + 2, …, LN, LN + 1, …, M]

The preverified phase downloads headers with block numbers from 0 to PN. It gets the headers that are known to be trusted in advance when the code was compiled. The point of splitting this phase is that verification is fastest here.

The linear phase continues to download from PN up to a certain block number - LN. LN is chosen in a way that the possibility of forks before LN is minimal. The point of splitting this phase is that verification is simpler and faster, because it can be implemented with more assumptions than in the forky phase.

Then a forky phase takes over. This phase tries to get all the remaining headers (and goes on forever). In this phase it is possible to observe a tree of forks, and branch death as the main branch continues.

## Phase 1: “preverified”

The source code contains a list of P + 1 hardcoded block hashes that are known to be valid (i.e. preverified):

* preverified hash of block 0
* preverified hash of block 192
* preverified hash of block 384
* …
* preverified hash of block P * 192

The GetBlockHeaders message allows requesting a continuous slice of headers, so we can request all blocks between the preverified ones:

* get headers slice in range [0, 192]
* get headers slice in range [192, 384]
* …

It is easy to verify each slice by comparing the hashes of the first and last headers to preverified hashes, and comparing that the child.parent_hash = hash(parent) for the intermediate headers. If the verification fails, the peer that sent this slice is penalized, and the slice is requested again (from a different peer).

Slice fetching and verification is performed in a number of stages. Each stage processes slices that reached an expected status (HeaderSliceStatus), and transitions them to the next status (according to the picture). Each stage operates on a set of slices in some state - input state. If there are no slices in the input state, it waits (using `await`). As soon as there are slices in this state, it awakes and processes them. Note that this is a pipeline that works in parallel: for example, if a slice A is Waiting and just received from sentry, slice B is already Downloaded before and needs to be verified, and slice C is already Verified and needs to be saved, all 3 operations on different slices can be done in parallel. This is beneficial, if A is network-bound, B is CPU-bound and C is IO-bound.

![](state_machine_preverified.png)

Storing all P slices in memory is not possible, so instead a sliding window approach is used. We allocate a window of W slices (HeaderSlices), and this window is moved forward from slice 0 to slice P - W. It is possible to control the size of W using `downloader.headers-mem-limit` parameter. When blocks at the left side of the window are fetched, verified and saved to the database, they are evicted from memory (RefillStage) and the window moves to the higher block numbers.

```
  S0  - saved and evicted
  S1  - saved and evicted
  S2  - saved and evicted
W[S3] - Empty
W[S4] - Waiting
W[S5] - Verified
  S6  - pending
  S7  - pending
  S8  - pending
  S9  - pending
```

In this example the window contains 3 slices from S3 to S5. The slices before S3 are already processed and saved. The slices after S5 are waiting to be processed in the future. The slices within the window can have any status: they start in the Empty status, and are processed according to the state machine above.

When all P slices are obtained, the process stops and switches to the next phase.

## Phase 2: “linear”

### TopBlockEstimateStage

We run a task that estimates M - the current known max block number. This task listens for incoming NewBlockHashes messages and takes the average. We shouldn’t take the maximum, because a rogue node might send a fake number.

### LN calculation

We need to estimate the minimum number of blocks T before block M where it is still possible to have forks. Then:

    LN = M - T

A conservative limit is T=90K blocks, because it is trusted that all forks from that time are consolidated into the canonical chain.

### Downloading

![](state_machine_linear.png)

Using the same approach as phase 1, we start downloading block headers from PN to LN. The verification stage is different, because we don’t have preverified hashes to rely on. Additional checks of the header structure validity are needed so that we don’t waste time on saving and redownloading:

* block numbers are sequential
* timestamps are the past, and monotonic
* difficulty matches the [CalcDifficulty formulas](https://github.com/ledgerwatch/erigon/blob/devel/consensus/ethash/consensus.go#L350)
* PoW is verified (see [verifySeal](https://github.com/ledgerwatch/erigon/blob/devel/consensus/ethash/consensus.go#L539) or [rust ethash](https://github.com/rust-ethereum/ethash))

Those verifications for blocks within a single slice can be performed on multiple slices in parallel (opportunistically). Such slices are not connected to each other yet, but they get an intermediate "VerifiedInternally" status. Once the leftmost slice reaches this status, it can be connected to the trusted parent, and transition to Verified.

In order to finalize the verification of a slice, we need to “connect” it to the left side of the sliding window using the formula `lefmost.parent_hash = hash(lefmost_parent)`, because the left side is assumed to be verified at all times. If it fails to connect, the peer who sent this slice is penalized, and the slice is requested again (from a different peer).

### Phase 3: “forky”

The goal of this phase is to download the last 90K headers of the canonical chain (CC).
There's a guarantee to have no forks prior to that, and possibility to have forks after (forming the tree branches).

Since there's no "last header", this phase can operate forever and follow the CC tip.
TimeoutStage makes sure to exit the downloader loop periodically,
and give breathe to the other sync stages.

The forky phase operates in a HeaderSlices window of a fixed size (roughly 100K headers).
When the window fills up with Saved slices, the window slides up.
This is done in 2 steps: ExtendStage appends a new Empty slice on top,
and when the loop exits by timeout, the bottom slice is trimmed.
Because of that there are always some Empty slices on top, and the window size is kept limited.

The algorithm starts similarly to the “linear” phase,
but VerifyLinkLinearStage is replaced with VerifyLinkForkyStage to handle forking.

#### Fork mode

At some point, during the linear slice link verification it encounters a header slice that can't connect to the canonical chain - a potential fork.
Such slice is marked with a Fork status, and the downloader switches to "fork mode" to handle that. The fork mode currently handles only 1 fork at a time.

It starts having a single Fork slice. This slice is potentially a part of a fork chain (FC).

There are 2 options:
 
* Either the FC is better than the CC (the top header has a larger total difficulty). In this case the algorithm needs to continue the FC down to find the connection point to the CC and switch to the fork.
* Or the FC is worse than the CC. In this case the algorithm needs to continue the existing CC further up, and discard the FC at some point.

The algorithm follows both paths: tries to continue the FC down, and tries to continue the CC up at the same time.

The FC is stored in a separate HeaderSlices instance - fork_header_slices.
Most of the logic in downloader stages is reused for FC except the link validation.

The link validation logic operates on both CC (header_slices) and FC (fork_header_slices).
For each slice it tries to link and extend the chains in the desired direction.

If extension succeeds, it can lead to the following terminal conditions:
* if a common header between FC and CC is found (a fork connection point), we can calculate the total difficulty of the FC. 
    If FC is better, the downloader terminates to unwind and switch to FC.
    If CC is better, FC is discarded.
* if FC reaches the bottom of CC, FC is discarded, and the downloader goes back to linear processing. 

#### Unwind

When the fork mode detects that the current CC needs to be replaced by a better FC,
it terminates and requests unwinding.

This happens in several steps:

1. A pending unwind request is created where a fork switch command is saved.
2. The stage execute() finishes with ExecOutput::Unwind.
3. The stage unwind() is called to wipe the old CC from the fork point.
4. When the stage execute() is called again, it finalizes unwinding by switching FC to CC.

### See also

* [erigon header downloader docs](https://github.com/ledgerwatch/erigon/wiki/Header-downloader)
