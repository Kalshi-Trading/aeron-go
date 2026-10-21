/*
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

import "errors"

// AddDestination adds a destination manually to a multi-destination-cast Publication
// (channel with control-mode=manual).
func (pub *Publication) AddDestination(endpointChannel string) error {
	if pub.IsClosed() {
		return errors.New("publication is closed")
	}
	return pub.conductor.AddDestination(pub.regID, endpointChannel)
}

// RemoveDestination removes a destination manually from a multi-destination-cast Publication.
func (pub *Publication) RemoveDestination(endpointChannel string) error {
	if pub.IsClosed() {
		return errors.New("publication is closed")
	}
	return pub.conductor.RemoveDestination(pub.regID, endpointChannel)
}
