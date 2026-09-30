package redistransport

import (
	"fmt"

	"github.com/dunglas/mercure"
)

// newCodec wraps mercure.NewCodec, prefixing its error with the package's
// "redis transport:" prefix.
func newCodec(encoding string) (mercure.Codec, error) {
	c, err := mercure.NewCodec(encoding)
	if err != nil {
		return nil, fmt.Errorf("redis transport: %w", err)
	}

	return c, nil
}
