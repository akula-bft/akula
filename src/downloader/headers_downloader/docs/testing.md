# Header downloader tests

The header downloader has a suite of functional tests in [downloader_tests.rs](../downloader_tests.rs)

Each test is doing the following:
1. Initializes an initial state of header_slices and dependency mocks (sentry and verifier);
2. Runs downloader.run() with the initial state;
3. Checks that the result state of produced header_slices and fork_header_slices matches expectations.

### Header slices descriptor

The initial and expected states are described using an ad-hoc test specification language.
This is used for describing both initial state of header_slices,
and the expected state of produced header_slices and fork_header_slices.

The language describes a sequence of header slices.

Example:

    +   +   +   #   -

Each symbol here identifies a HeaderSliceStatus.
This line describes HeaderSlices with 5 slices with given statuses.
For example, `+` denotes a Saved slice.  
See `TryFrom<char> for HeaderSliceStatus` for the full list.

By default, the first slice starts with block number 0,
the second slice starts from a block number 192 (HEADER_SLICE_SIZE) etc.
It is possible to start higher: `_` symbol denotes a skipped slice.

Example:

    _   +   +

This describes HeaderSlices with 2 Saved slices,
and the first slice starts from a block number 192.

### Sentry slices descriptor

The sentry descriptor is similar to the header slices descriptor,
but uses just the Downloaded (`.`) status symbol.

For example:

    .   _   .

This denotes that 2 slices are available for download: [0..192] and [384..576].
Requesting the slice [192..384] is not going to return anything.
SentryClientMock ignores the request if the headers are not found. 

### Verification IDs

Each header generated for tests gets an ID that is used for verification during testing. 
Link verification succeeds if the blocks have sequential IDs.

By default, the IDs match the block numbers,
so link verification always succeeds for adjacent slices
since the block numbers are sequential.

Custom IDs are denoted as 'a, 'b, 'c etc.
For each slice headers a range of IDs is assigned:
'a = [0..192], 'b = [192..384], 'c = [384..576] etc.

This allows breaking verification on purpose,
for example, 'c can be linked to 'd, but not to 'f,
while 'e can be linked to 'f.

Example 1:

    .   .   .

is equivalent to:

    .'a .'b .'c

The slices are sequential, and the link verification must succeed.

Example 2:

    _   .   .   .'k .

is equivalent to:

    _   .'b .'c .'k .'e

The zeroth slice is skipped, so the first slice starts with 'b.
The last slice has no custom ID, so it gets a default ID
corresponding to the block number - 'e.

In this case slices 'b-to-'c can be linked, but 'c-to-'k and 'k-to-'e - can't. 

### Fork connection headers

It is possible to have a fork chain slice that can be linked to both 
canonical chain on the left, and the fork chain on the right.
Such slice has a fork connection header somewhere inside it.
After this header it "forks" to the fork chain.

A back-tick is used to denote such a slice.

For example:

    .   .`c .'d 

means that at ID position 'b we have a slice
that has IDs starting normally (from 'b), but ending with IDs as if it was at 'c.
Such slice can be connected to slice 'a on the left, and 'd on the right.

The start ID is assumed based on the position ('a, 'b etc.),
only the alternative end ID is specified.

Technically this modifies the IDs of the last portion of the slice (currently just the last header).
For example, if 'c normally means IDs range [384..576],
then `` `c `` at position 'b ('b = [192..384]) means that the slice gets IDs in range [192..576]
In practice: 192, 193, ... 382, 575
(i.e. the last 'b header ID 383 is replaced with the last 'c header ID 575)
In this case header 382 is the fork connection header.

## Implementation details

`downloader.run()` loop runs for as long as any of the downloader stages can proceed
further by updating a state of some slice,
and the termination condition (is_over_check) is not met.

During testing, it tries to download the Empty slices,
and sends requests to the SentryClientMock.
When SentryClientMock handles a request for some slice,
it will remember that this particular response is sent.

When all configured responses are sent,
the SentryClientMock closes its side of the stream bubbling to SentryClientReactor
(see make_receive_stream / receive_messages_senders_dropper in SentryClientReactor).
Then message_stream FetchReceiveStage is going to produce None,
and render FetchReceiveStage `can_proceed = false`.

If FetchReceiveStage can't proceed while there are still some Waiting slices,
none of the other stages can proceed, therefore the downloader stage loop exits,
and the test can finish.

Having the slide/refill logic there's always an Empty slice to come,
and since the SentryClientMock has a limited configured supply of responses,
the FetchReceiveStage progress is going stop,
and the loop will exit at some point.

It can be useful to debug this operation by enabling `trace!` messages in DownloaderStageLoop.
