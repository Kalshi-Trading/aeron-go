// Deprecated aliases kept for one release after the Id -> ID rename; remove in the next breaking release.

package aeron

// Deprecated: use ID.
func (c *Counter) Id() int32 {
	return c.ID()
}

// Deprecated: use CorrelationID.
func (c *Counter) CorrelationId() int64 {
	return c.CorrelationID()
}

// Deprecated: use RegistrationID.
func (c *Counter) RegistrationId() int64 {
	return c.RegistrationID()
}

// Deprecated: use ChannelStatusID.
func (sub *Subscription) ChannelStatusId() int32 {
	return sub.ChannelStatusID()
}

// Deprecated: use RemoveAvailableCounterHandlerByID.
func (aeron *Aeron) RemoveAvailableCounterHandlerById(registrationId int64) bool {
	return aeron.RemoveAvailableCounterHandlerByID(registrationId)
}

// Deprecated: use RemoveUnavailableCounterHandlerByID.
func (aeron *Aeron) RemoveUnavailableCounterHandlerById(registrationId int64) bool {
	return aeron.RemoveUnavailableCounterHandlerByID(registrationId)
}

// Deprecated: use RemoveAvailableCounterHandlerByID.
func (cc *ClientConductor) RemoveAvailableCounterHandlerById(registrationId int64) bool {
	return cc.RemoveAvailableCounterHandlerByID(registrationId)
}

// Deprecated: use RemoveUnavailableCounterHandlerByID.
func (cc *ClientConductor) RemoveUnavailableCounterHandlerById(registrationId int64) bool {
	return cc.RemoveUnavailableCounterHandlerByID(registrationId)
}

// Deprecated: use AddSessionID.
func AddSessionId(channel string, sessionId int32) (string, error) {
	return AddSessionID(channel, sessionId)
}

// Deprecated: use IsSessionIDTagged.
func (b *ChannelUriStringBuilder) IsSessionIdTagged(isSessionIdTagged bool) *ChannelUriStringBuilder {
	return b.IsSessionIDTagged(isSessionIdTagged)
}

// Deprecated: use GetIsSessionIDTagged.
func (b *ChannelUriStringBuilder) GetIsSessionIdTagged() bool {
	return b.GetIsSessionIDTagged()
}

// Deprecated: use InitialTermID.
func (b *ChannelUriStringBuilder) InitialTermId(initialTermId int32) *ChannelUriStringBuilder {
	return b.InitialTermID(initialTermId)
}

// Deprecated: use GetInitialTermID.
func (b *ChannelUriStringBuilder) GetInitialTermId() (int32, bool) {
	return b.GetInitialTermID()
}

// Deprecated: use TermID.
func (b *ChannelUriStringBuilder) TermId(termId int32) *ChannelUriStringBuilder {
	return b.TermID(termId)
}

// Deprecated: use GetTermID.
func (b *ChannelUriStringBuilder) GetTermId() (int32, bool) {
	return b.GetTermID()
}

// Deprecated: use SessionID.
func (b *ChannelUriStringBuilder) SessionId(sessionId int32) *ChannelUriStringBuilder {
	return b.SessionID(sessionId)
}

// Deprecated: use TaggedSessionID.
func (b *ChannelUriStringBuilder) TaggedSessionId(tag int64) *ChannelUriStringBuilder {
	return b.TaggedSessionID(tag)
}

// Deprecated: use GetSessionID.
func (b *ChannelUriStringBuilder) GetSessionId() (int64, bool) {
	return b.GetSessionID()
}

// Deprecated: use ResponseCorrelationID.
func (b *ChannelUriStringBuilder) ResponseCorrelationId(correlationId int64) *ChannelUriStringBuilder {
	return b.ResponseCorrelationID(correlationId)
}

// Deprecated: use ResponseCorrelationIDString.
func (b *ChannelUriStringBuilder) ResponseCorrelationIdString(correlationId string) *ChannelUriStringBuilder {
	return b.ResponseCorrelationIDString(correlationId)
}

// Deprecated: use GetResponseCorrelationID.
func (b *ChannelUriStringBuilder) GetResponseCorrelationId() string {
	return b.GetResponseCorrelationID()
}

// Deprecated: use StreamID.
func (b *ChannelUriStringBuilder) StreamId(streamId int32) *ChannelUriStringBuilder {
	return b.StreamID(streamId)
}

// Deprecated: use GetStreamID.
func (b *ChannelUriStringBuilder) GetStreamId() (int32, bool) {
	return b.GetStreamID()
}
