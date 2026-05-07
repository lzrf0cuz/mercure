package redistransport

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pumpMiniredis keeps miniredis's blocking XREADGROUP progressing by advancing
// its clock until the returned stop function is called.
func pumpMiniredis(mr *miniredis.Miniredis) func() {
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				mr.FastForward(20 * time.Millisecond)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	return func() { close(stop) }
}

// TestDispatchUsesTopicIndexNotSkipfilter proves the live dispatch path matches
// via the topic index, not the skipfilter SubscriberList. After a normal
// AddSubscriber (which populates both structures), the subscriber is removed
// from ONLY the skipfilter, leaving the index intact. A subsequent dispatch must
// still reach it — because the index is the dispatch matcher. If dispatch ever
// reverts to SubscriberList.MatchAny, the poisoned skipfilter yields an empty
// match and the subscriber misses the event.
//
// Run with explicit shard counts so BOTH the single-shard (dispatchToSubscribers)
// and sharded (shardWorker) index paths are pinned regardless of NumCPU.
func TestDispatchUsesTopicIndexNotSkipfilter(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			t.Parallel()

			transport, mr := newTestTransport(t, WithDispatchShards(shards))
			tss := testTopicSelectorStore()
			ctx := context.Background()

			const topic = "https://example.com/index-wiring"

			sub := mercure.NewLocalSubscriber("", testLogger(), tss)
			sub.SetTopics([]string{topic}, nil)
			require.NoError(t, transport.AddSubscriber(ctx, sub))

			// Poison the skipfilter: remove from it directly, leaving the index
			// intact. Dispatch must still deliver via the index.
			transport.shards[transport.shardFor(sub.ID)].subscribers.Remove(sub)

			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{topic},
				Event:  mercure.Event{Data: "via-index"},
			}))

			stop := pumpMiniredis(mr)
			defer stop()

			select {
			case got := <-sub.Receive():
				assert.Equal(t, "via-index", got.Data)
			case <-time.After(5 * time.Second):
				t.Fatal("subscriber missed the event — live dispatch did not match via the topic index")
			}
		})
	}
}

// TestRemoveSubscriberClearsIndex proves removeSubscriberFromList unindexes the
// subscriber: after RemoveSubscriber, a dispatch must NOT reach it.
//
// The negative is made sound under multi-shard execution by a two-event sentinel:
// the witness subscribes to a DISTINCT topic and the sentinel is published after
// the main event. Because the single listener processes stream entries
// sequentially and each shardedDispatch is synchronous (waits for every shard),
// the sentinel's fan-out begins only after the main event's fan-out completed
// across ALL shards. So "witness received the sentinel" proves the removed
// subscriber's shard already ran for the main event — regardless of which shards
// the two subscribers landed on. Pinned at explicit shard counts.
func TestRemoveSubscriberClearsIndex(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			t.Parallel()

			transport, mr := newTestTransport(t, WithDispatchShards(shards))
			tss := testTopicSelectorStore()
			ctx := context.Background()

			const (
				topic    = "https://example.com/index-remove"
				sentinel = "https://example.com/index-remove-sentinel"
			)

			gone := mercure.NewLocalSubscriber("", testLogger(), tss)
			gone.SetTopics([]string{topic}, nil)
			require.NoError(t, transport.AddSubscriber(ctx, gone))

			witness := mercure.NewLocalSubscriber("", testLogger(), tss)
			witness.SetTopics([]string{sentinel}, nil)
			require.NoError(t, transport.AddSubscriber(ctx, witness))

			require.NoError(t, transport.RemoveSubscriber(ctx, gone))

			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{topic},
				Event:  mercure.Event{Data: "after-remove"},
			}))
			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{sentinel},
				Event:  mercure.Event{Data: "sentinel"},
			}))

			stop := pumpMiniredis(mr)
			defer stop()

			select {
			case got := <-witness.Receive():
				require.Equal(t, "sentinel", got.Data)
			case <-time.After(5 * time.Second):
				t.Fatal("witness never received the sentinel — listener did not progress")
			}

			// The main event's fan-out is now complete across all shards.
			select {
			case got := <-gone.Receive():
				t.Fatalf("removed subscriber received %q — index was not cleared on remove", got.Data)
			default:
			}
		})
	}
}
