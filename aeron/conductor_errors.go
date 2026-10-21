/*
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
)

// ErrClientClosed is returned by operations on an Aeron client that has been closed or is terminating.
var ErrClientClosed = errors.New("aeron client is closed")

// ErrUnknownRegistration is returned when polling a registration id this client never issued (or already consumed).
var ErrUnknownRegistration = errors.New("unknown registration id")
