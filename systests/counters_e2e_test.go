// Copyright 2026 Kalshi Trading
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package systests

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Counters E2E against the Java 1.53.3 driver from docker/docker-compose.yml. Run with:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 go test ./systests/ -run TestCountersE2E -v

func countersE2EDir(t *testing.T) string {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker Java driver")
	}
	if dir := os.Getenv("AERON_DIR"); dir != "" {
		return dir
	}
	return "/dev/shm/aeron-compose"
}

func countersCompose(t *testing.T, args ...string) *exec.Cmd {
	base := []string{"compose", "-p", "aeron-counters", "-f", "../docker/counters.compose.yml"}
	cmd := exec.Command("docker", append(base, args...)...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("UID=%d", os.Getuid()), fmt.Sprintf("GID=%d", os.Getgid()))
	return cmd
}

func runCountersCompose(t *testing.T, args ...string) string {
	out, err := countersCompose(t, args...).CombinedOutput()
	require.NoError(t, err, string(out))
	return string(out)
}

// ensureCountersImages builds the compose images only when missing (avoids Docker Hub rate limits on rebuilds).
func ensureCountersImages(t *testing.T) {
	if exec.Command("docker", "image", "inspect", "aeron-counters-aeron-stat", "aeron-counters-counters-tool").Run() != nil {
		runCountersCompose(t, "build", "--quiet")
	}
}

func connectE2E(t *testing.T, dir string) *aeron.Aeron {
	a, err := aeron.Connect(aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second))
	require.NoError(t, err)
	return a
}

type counterHandlerFunc func(reader *counters.Reader, registrationId int64, counterId int32)

func (f counterHandlerFunc) Handle(reader *counters.Reader, registrationId int64, counterId int32) {
	f(reader, registrationId, counterId)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func aeronStatLine(stat string, label string) (int32, int64, bool) {
	re := regexp.MustCompile(`(?m)^\s*(\d+):\s+([\d,-]+) - ` + regexp.QuoteMeta(label) + `\s*$`)
	m := re.FindStringSubmatch(stat)
	if m == nil {
		return 0, 0, false
	}
	id, _ := strconv.Atoi(m[1])
	v, _ := strconv.ParseInt(strings.ReplaceAll(m[2], ",", ""), 10, 64)
	return int32(id), v, true
}

// Go creates client owned and static counters; Java AeronStat must see them with the right values, and static
// counters must dedupe across Go clients, Go client restarts and Java clients.
func TestCountersE2EGoToJava(t *testing.T) {
	dir := countersE2EDir(t)
	ensureCountersImages(t)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	staticRegId := time.Now().UnixNano()
	const typeId, keyedTypeId, staticTypeId = int32(9101), int32(9102), int32(9103)
	label := "go-e2e-counter-" + suffix
	keyedLabel := "go-e2e-keyed-" + suffix
	staticLabel := "go-e2e-static-" + suffix

	client := connectE2E(t, dir)
	reader := client.CounterReader()

	counter, err := client.AddCounterByLabel(typeId, label)
	require.NoError(t, err)
	counter.Set(424242)
	assert.True(t, counter.IsClientOwned())
	assert.Equal(t, client.ClientID(), reader.GetCounterOwnerID(counter.ID()))
	assert.Equal(t, counter.RegistrationID(), reader.GetCounterRegistrationID(counter.ID()))

	key := atomic.MakeBuffer(make([]byte, 8))
	key.PutInt64(0, 0x1234567890)
	labelBuf := atomic.MakeBuffer([]byte(keyedLabel))
	keyed, err := client.AddCounter(keyedTypeId, key, 0, 8, labelBuf, 0, labelBuf.Capacity())
	require.NoError(t, err)
	keyed.Increment()
	keyed.GetAndAdd(76)
	v, err := reader.GetKeyPartInt64(keyed.ID(), 0)
	require.NoError(t, err)
	assert.EqualValues(t, 0x1234567890, v)
	assert.Equal(t, keyedTypeId, reader.GetCounterTypeID(keyed.ID()))
	assert.Equal(t, keyedLabel, reader.GetCounterLabel(keyed.ID()))
	assert.Equal(t, counters.RecordAllocated, reader.GetCounterState(keyed.ID()))
	assert.Equal(t, counters.NotFreeToReuse, reader.GetFreeForReuseDeadline(keyed.ID()))

	static1, err := client.AddStaticCounterByLabel(staticTypeId, staticLabel, staticRegId)
	require.NoError(t, err)
	static2, err := client.AddStaticCounterByLabel(staticTypeId, "ignored-label", staticRegId)
	require.NoError(t, err)
	assert.Equal(t, static1.ID(), static2.ID(), "static counters must dedupe")
	assert.NotEqual(t, static1.CorrelationID(), static2.CorrelationID())
	assert.False(t, static1.IsClientOwned())
	assert.Equal(t, staticRegId, static1.RegistrationID())
	assert.EqualValues(t, aeron.NullValue, reader.GetCounterOwnerID(static1.ID()))
	assert.Equal(t, static1.ID(), reader.FindByTypeIDAndRegistrationID(staticTypeId, staticRegId))
	static1.Set(99)

	// Non-static collision: the driver rejects a static counter for an existing client owned counter.
	_, err = client.AddStaticCounterByLabel(typeId, "collide", counter.RegistrationID())
	assert.ErrorContains(t, err, "non-static counter exists")

	stat := runCountersCompose(t, "run", "--rm", "aeron-stat")
	id, val, ok := aeronStatLine(stat, label)
	require.True(t, ok, "AeronStat output missing %s:\n%s", label, stat)
	assert.Equal(t, counter.ID(), id)
	assert.EqualValues(t, 424242, val)
	id, val, ok = aeronStatLine(stat, keyedLabel)
	require.True(t, ok, "AeronStat output missing %s", keyedLabel)
	assert.Equal(t, keyed.ID(), id)
	assert.EqualValues(t, 77, val)
	id, val, ok = aeronStatLine(stat, staticLabel)
	require.True(t, ok, "AeronStat output missing %s", staticLabel)
	assert.Equal(t, static1.ID(), id)
	assert.EqualValues(t, 99, val)
	t.Logf("AeronStat saw: counter=%d keyed=%d static=%d", counter.ID(), keyed.ID(), static1.ID())

	// Java addStaticCounter with the same (typeId, registrationId) must return the same counter.
	out := runCountersCompose(t, "run", "--rm", "counters-tool", "add-static",
		strconv.Itoa(int(staticTypeId)), strconv.FormatInt(staticRegId, 10), "java-ignored")
	assert.Contains(t, out, fmt.Sprintf("STATIC id=%d registrationId=%d value=99", static1.ID(), staticRegId))

	// Closing the Go client frees client owned counters but not the static counter.
	counterId := counter.ID()
	require.NoError(t, client.Close())
	client = connectE2E(t, dir)
	defer client.Close()
	reader = client.CounterReader()
	waitFor(t, 10*time.Second, "client owned counter to be freed", func() bool {
		return reader.GetCounterState(counterId) != counters.RecordAllocated ||
			reader.GetCounterLabel(counterId) != label
	})
	assert.Equal(t, counters.RecordAllocated, reader.GetCounterState(static1.ID()))
	static3, err := client.AddStaticCounterByLabel(staticTypeId, staticLabel, staticRegId)
	require.NoError(t, err)
	assert.Equal(t, static1.ID(), static3.ID())
	assert.EqualValues(t, 99, static3.Get())
}

// Java creates a counter; Go must observe it via the reader and the available/unavailable counter handlers.
func TestCountersE2EJavaToGo(t *testing.T) {
	dir := countersE2EDir(t)
	ensureCountersImages(t)

	label := "java-e2e-counter-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	const typeId = int32(9104)

	client := connectE2E(t, dir)
	defer client.Close()
	reader := client.CounterReader()

	available := make(chan [2]int64, 64)
	unavailable := make(chan [2]int64, 64)
	client.AddAvailableCounterHandler(counterHandlerFunc(func(r *counters.Reader, regId int64, counterId int32) {
		if r.GetCounterTypeID(counterId) == typeId {
			available <- [2]int64{regId, int64(counterId)}
		}
	}))
	client.AddUnavailableCounterHandler(counterHandlerFunc(func(r *counters.Reader, regId int64, counterId int32) {
		unavailable <- [2]int64{regId, int64(counterId)}
	}))

	cmd := countersCompose(t, "run", "--rm", "-T", "counters-tool", "add", strconv.Itoa(int(typeId)), label,
		"31337", "4000")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	javaLine := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "COUNTER ") {
				javaLine <- sc.Text()
			}
		}
	}()

	var javaId int32
	var javaRegId int64
	select {
	case line := <-javaLine:
		_, err := fmt.Sscanf(line, "COUNTER id=%d registrationId=%d", &javaId, &javaRegId)
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for Java counter")
	}

	select {
	case got := <-available:
		assert.Equal(t, [2]int64{javaRegId, int64(javaId)}, got)
	case <-time.After(10 * time.Second):
		t.Fatal("available counter handler not called for Java counter")
	}

	found := counters.NullCounterId
	reader.ForEachMetaData(func(counterId int32, tId int32, _ *atomic.Buffer, l string) {
		if l == label && tId == typeId {
			found = counterId
		}
	})
	require.Equal(t, javaId, found)
	waitFor(t, 5*time.Second, "java value", func() bool { return reader.GetCounterValue(javaId) == 31337 })
	t.Logf("java counter id=%d registrationId=%d valuesRegId=%d owner=%d", javaId, javaRegId, reader.GetCounterRegistrationID(javaId), reader.GetCounterOwnerID(javaId))
	assert.Equal(t, javaRegId, reader.GetCounterRegistrationID(javaId))
	// Registration ids are not unique across counter types (e.g. static counters), so match on type too.
	assert.Equal(t, javaId, reader.FindByTypeIDAndRegistrationID(typeId, javaRegId))
	assert.NotEqual(t, client.ClientID(), reader.GetCounterOwnerID(javaId))
	rc, err := counters.NewReadableRegisteredCounter(reader, javaRegId, javaId)
	require.NoError(t, err)
	assert.EqualValues(t, 31337, rc.Get())
	assert.Equal(t, label, rc.Label())

	deadline := time.After(60 * time.Second)
	for {
		select {
		case got := <-unavailable:
			if got == [2]int64{javaRegId, int64(javaId)} {
				require.NoError(t, cmd.Wait())
				return
			}
		case <-deadline:
			t.Fatal("unavailable counter handler not called after Java closed its counter")
		}
	}
}
