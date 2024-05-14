// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/siderolabs/go-circular"
)

func TestNextInterval(t *testing.T) {
	t.Parallel()

	opts := circular.PersistenceOptions{
		FlushInterval: 10 * time.Second,
		FlushJitter:   0.1,
	}

	var previous time.Duration

	for range 100 {
		interval := opts.NextInterval()

		assert.NotEqual(t, previous, interval)

		previous = interval

		assert.InDelta(t, opts.FlushInterval, interval, 0.1*float64(opts.FlushInterval))
	}
}
