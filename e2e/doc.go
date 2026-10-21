// Package e2e contains end-to-end test clients exercising aeron-go against a real Java Aeron media driver
// and archive (see docker/docker-compose.yml). The binaries under e2e/cmd are thin flag wrappers around
// this package so the same code runs from `go test` (AERON_E2E=1) and from the `e2e` compose profile.
package e2e
