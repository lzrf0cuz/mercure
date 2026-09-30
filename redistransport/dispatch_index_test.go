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

// TestDispatchUsesTopicIndexNotSkipfilter pins that live dispatch matches
// through the topic index, not the skipfilter SubscriberList: a subscriber
// removed from the skipfilter alone must still receive the event. Shard
// counts 1 and 4 cover dispatchToSubscribers and shardWorker regardless of
// NumCPU.
func TestDispatchUsesTopicIndexNotSkipfilter(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			t.Parallel()

			transport, mr := newTestTransport(t, WithDispatchShards(shards))
			tss := testTopicMatcherStore()
			ctx := context.Background()

			const topic = "https://example.com/index-wiring"

			sub := mercure.NewLocalSubscriber("", testLogger(), tss)
			sub.SetMatchers(topicMatchers([]string{topic}), nil)
			require.NoError(t, transport.AddSubscriber(ctx, sub))

			// Poison the skipfilter: remove from it directly, leaving the index
			// intact. Dispatch must still deliver via the index.
			transport.shards[transport.shardFor(sub.ID)].subscribers.Remove(sub)

			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{topic},
				Data:   "via-index",
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
// The witness subscribes to a distinct topic and its sentinel is published
// after the main event. The listener processes entries in order and each
// shardedDispatch waits for every shard, so the witness receiving the sentinel
// proves the main event's fan-out finished on every shard.
func TestRemoveSubscriberClearsIndex(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			t.Parallel()

			transport, mr := newTestTransport(t, WithDispatchShards(shards))
			tss := testTopicMatcherStore()
			ctx := context.Background()

			const (
				topic    = "https://example.com/index-remove"
				sentinel = "https://example.com/index-remove-sentinel"
			)

			gone := mercure.NewLocalSubscriber("", testLogger(), tss)
			gone.SetMatchers(topicMatchers([]string{topic}), nil)
			require.NoError(t, transport.AddSubscriber(ctx, gone))

			witness := mercure.NewLocalSubscriber("", testLogger(), tss)
			witness.SetMatchers(topicMatchers([]string{sentinel}), nil)
			require.NoError(t, transport.AddSubscriber(ctx, witness))

			require.NoError(t, transport.RemoveSubscriber(ctx, gone))

			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{topic},
				Data:   "after-remove",
			}))
			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{sentinel},
				Data:   "sentinel",
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
