package redistransport

import (
	"fmt"

	"github.com/dunglas/mercure"
)

// newCodec dispatches to mercure.NewCodec and wraps the upstream error with
// a redistransport: prefix so callers can grep logs by transport. The
// concrete codec types and the Codec interface itself live in upstream's
// mercure package — this transport adds no codec semantics of its own.
func newCodec(encoding string) (mercure.Codec, error) {
	c, err := mercure.NewCodec(encoding)
	if err != nil {
		return nil, fmt.Errorf("redistransport: %w", err)
	}

	return c, nil
}
