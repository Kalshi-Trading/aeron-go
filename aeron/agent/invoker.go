package agent

// Invoker drives an Agent from the caller's goroutine (Agrona AgentInvoker). Not safe for concurrent use.
type Invoker struct {
	agent        Agent
	errorHandler ErrorHandler
	errorCounter ErrorCounter
	isStarted    bool
	isRunning    bool
	isClosed     bool
}

// NewInvoker creates an Invoker; errorHandler and errorCounter may be nil.
func NewInvoker(errorHandler ErrorHandler, errorCounter ErrorCounter, agent Agent) *Invoker {
	return &Invoker{agent: agent, errorHandler: errorHandler, errorCounter: errorCounter}
}

func (i *Invoker) Agent() Agent    { return i.agent }
func (i *Invoker) IsStarted() bool { return i.isStarted }
func (i *Invoker) IsRunning() bool { return i.isRunning }
func (i *Invoker) IsClosed() bool  { return i.isClosed }

// Start calls OnStart once. If it fails the invoker is closed.
func (i *Invoker) Start() {
	if i.isStarted {
		return
	}
	i.isStarted = true
	if err := i.agent.OnStart(); err != nil {
		i.isRunning = true
		i.handleError(err)
		i.Close()
		return
	}
	i.isRunning = true
}

// Invoke runs one duty cycle if running and returns the work count. ErrAgentTermination closes the invoker.
func (i *Invoker) Invoke() int {
	if !i.isRunning {
		return 0
	}
	n, err := i.agent.DoWork()
	if err != nil {
		if isTermination(err) {
			i.isRunning = false
			i.handleError(err)
			i.Close()
		} else {
			i.handleError(err)
		}
	}
	return n
}

// Close calls OnClose once.
func (i *Invoker) Close() {
	if i.isClosed {
		return
	}
	i.isRunning = false
	i.isClosed = true
	i.agent.OnClose()
}

func (i *Invoker) handleError(err error) {
	if i.errorCounter != nil && i.isRunning && !isTermination(err) {
		i.errorCounter.Increment()
	}
	if i.errorHandler != nil {
		i.errorHandler(err)
	}
}
