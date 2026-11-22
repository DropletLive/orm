package orm

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestIsDuplicateInProcess(t *testing.T) {
	inProcess := &jetstream.APIError{Code: 409, ErrorCode: jsErrCodeDuplicateInProcess, Description: "duplicate message id is in process"}

	assert.True(t, isDuplicateInProcess(inProcess))
	assert.True(t, isDuplicateInProcess(fmt.Errorf("ack: %w", inProcess)))
	assert.False(t, isDuplicateInProcess(&jetstream.APIError{Code: 503, ErrorCode: jetstream.JSErrCodeStreamNotFound}))
	assert.False(t, isDuplicateInProcess(errors.New("nats: timeout")))
	assert.False(t, isDuplicateInProcess(nil))
}
