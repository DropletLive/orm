package elasticsearch

import (
	"encoding/json"
	"math"
	"strconv"
)

// Decoded with UseNumber, so parse json.Number digits to keep snowflake IDs (>2^53) exact; float64 is a fallback.
func AsUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case json.Number:
		if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
			return u, true
		}

		if f, err := n.Float64(); err == nil {
			return uint64(math.Round(f)), true
		}
	case float64:
		return uint64(math.Round(n)), true
	}

	return 0, false
}

func AsFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()

		return f, err == nil
	case float64:
		return n, true
	}

	return 0, false
}
