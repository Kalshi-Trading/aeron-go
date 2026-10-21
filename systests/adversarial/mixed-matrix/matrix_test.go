package mixedmatrix

import (
	"os"
	"testing"
	"time"
)

// TestMatrixChurn runs every publisher type and every consumer type over every transport (each (publisher,
// transport) pair once, the consumer rotating so every (consumer, transport) and (publisher, consumer) pair is
// covered too) under seeded random churn: publishers of the cell's and other types start, close gracefully, get
// SIGKILLed and SIGSTOPped; Go and Java subscribers join late, leave and get SIGKILLed; clients reconnect. Each
// transport gets a fresh driver; driver counters must return to their baseline afterwards.
func TestMatrixChurn(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	cellDuration := 6 * time.Second
	if d, err := time.ParseDuration(os.Getenv("MM_CELL")); err == nil {
		cellDuration = d
	}
	for ti, tr := range transports {
		tr := tr
		ti := ti
		t.Run(tr.name, func(t *testing.T) {
			base := snapshot()
			freshStack(t)
			counters := driverCounters(t)
			for pi, pt := range pubTypes {
				consumer := consumerTypes[(ti+pi)%len(consumerTypes)]
				t.Run(pt.String()+"/"+consumer, func(t *testing.T) {
					runCell(t, rnd, cell{pub: pt, consumer: consumer, transport: tr, stream: int32(9700 + ti*10 + pi),
						port0: 27700 + 2*pi, port1: 27701 + 2*pi, duration: cellDuration, mixed: true})
				})
				if t.Failed() {
					return
				}
			}
			requireCountersReturn(t, counters)
			requireNoLeaks(t, base)
		})
	}
}
