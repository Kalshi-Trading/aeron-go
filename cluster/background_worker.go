package cluster

// BackgroundWorker may optionally be implemented by a ClusteredService to perform work while the agent is otherwise
// idle (Java ClusteredService.doBackgroundWork). It is invoked on every duty cycle with the current time in
// nanoseconds and returns the amount of work done. The service must not modify state derived from the log here.
type BackgroundWorker interface {
	DoBackgroundWork(nowNs int64) int
}
