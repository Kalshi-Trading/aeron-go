package client

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lirm/aeron-go/aeron"
)

type memberIngress struct {
	ctx            *Context
	memberId       int32
	endpoint       string
	registrationId int64
	publication    aeron.Publisher
	publicationErr error
}

func newMemberIngress(ctx *Context, memberId int32, endpoint string) *memberIngress {
	return &memberIngress{ctx: ctx, memberId: memberId, endpoint: endpoint, registrationId: nullValue}
}

func (m *memberIngress) asyncAddPublication() {
	channel, err := ingressChannelForEndpoint(m.ctx, m.endpoint)
	if err == nil {
		m.registrationId, err = asyncAddIngressPublication(m.ctx, channel, m.ctx.IngressStreamId)
	}
	if err != nil {
		m.publicationErr = err
		m.registrationId = nullValue
	}
	m.publication = nil
}

func (m *memberIngress) asyncGetPublication() {
	if m.registrationId == nullValue {
		return
	}
	pub, err := getIngressPublication(m.ctx, m.registrationId)
	if err != nil {
		if !isResourceTemporarilyUnavailable(err) {
			m.publicationErr = err
		}
		m.registrationId = nullValue
		return
	}
	if pub != nil {
		m.publication = pub
		m.registrationId = nullValue
	}
}

func (m *memberIngress) close() {
	if m.publication != nil {
		if err := m.publication.Close(); err != nil {
			logger.Debugf("error closing ingress publication memberId=%d: %v", m.memberId, err)
		}
	} else if m.registrationId != nullValue {
		asyncRemovePublication(m.ctx, m.registrationId)
	}
	m.registrationId = nullValue
	m.publication = nil
}

func (m *memberIngress) String() string {
	return fmt.Sprintf("MemberIngress{memberId=%d, endpoint=%s, publication=%v}", m.memberId, m.endpoint,
		m.publication != nil)
}

// ParseIngressEndpoints parses "memberId=host:port,..." into a map of memberId to endpoint.
func ParseIngressEndpoints(endpoints string) (map[int32]string, error) {
	result := make(map[int32]string)
	if endpoints == "" {
		return result, nil
	}
	for _, endpoint := range strings.Split(endpoints, ",") {
		i := strings.IndexByte(endpoint, '=')
		if i < 0 {
			return nil, fmt.Errorf("%w: endpoint missing '=' separator: %s", ErrConfiguration, endpoints)
		}
		memberId, err := strconv.ParseInt(strings.TrimSpace(endpoint[:i]), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid member id in endpoint %q: %v", ErrConfiguration, endpoint, err)
		}
		result[int32(memberId)] = endpoint[i+1:]
	}
	return result, nil
}

func parseMemberIngress(ctx *Context, endpoints string) (map[int32]*memberIngress, error) {
	parsed, err := ParseIngressEndpoints(endpoints)
	if err != nil {
		return nil, err
	}
	members := make(map[int32]*memberIngress, len(parsed))
	for id, endpoint := range parsed {
		members[id] = newMemberIngress(ctx, id, endpoint)
	}
	return members, nil
}

func closeAllMembers(members map[int32]*memberIngress) {
	for _, m := range members {
		m.close()
	}
}

func ingressChannelForEndpoint(ctx *Context, endpoint string) (string, error) {
	uri, err := aeron.ParseChannelUri(ctx.IngressChannel)
	if err != nil {
		return "", err
	}
	if uri.IsUdp() {
		uri.Set("endpoint", endpoint)
	}
	return uri.String(), nil
}

func asyncAddIngressPublication(ctx *Context, channel string, streamId int32) (int64, error) {
	if ctx.IsIngressExclusive {
		return ctx.Aeron.AsyncAddExclusivePublication(channel, streamId)
	}
	return ctx.Aeron.AsyncAddPublication(channel, streamId)
}

func getIngressPublication(ctx *Context, registrationId int64) (aeron.Publisher, error) {
	if ctx.IsIngressExclusive {
		pub, err := ctx.Aeron.GetExclusivePublication(registrationId)
		if pub == nil || err != nil {
			return nil, err
		}
		return pub, nil
	}
	pub, err := ctx.Aeron.GetPublication(registrationId)
	if pub == nil || err != nil {
		return nil, err
	}
	return pub, nil
}

// asyncRemovePublication releases a publication whose async add has not been resolved by getIngressPublication,
// whether or not the driver has responded yet (Java Aeron.asyncRemovePublication).
func asyncRemovePublication(ctx *Context, registrationId int64) {
	if ctx.Aeron == nil || ctx.Aeron.IsClosed() {
		return
	}
	if err := ctx.Aeron.AsyncRemovePublication(registrationId); err != nil {
		ctx.ErrorHandler(err)
	}
}

// isResourceTemporarilyUnavailable reports a registration error that Java retries rather than throws
// (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE).
func isResourceTemporarilyUnavailable(err error) bool {
	var regErr *aeron.RegistrationError
	return errors.As(err, &regErr) && regErr.ErrorCode() == aeron.ErrorCodeResourceTemporarilyUnavailable
}
