/*
Copyright 2016 Stanislav Liberman
Copyright (C) 2022 Talos, Inc.

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

type imageRejecter interface {
	RejectImage(imageCorrelationID int64, position int64, reason string) (int64, error)
}

// rejectImage sends the REJECT_IMAGE command through the driver proxy.
func (cc *ClientConductor) rejectImage(imageCorrelationID int64, position int64, reason string) error {
	logger.Debugf("RejectImage: imageCorrelationID=%d position=%d reason=%s", imageCorrelationID, position, reason)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	rejecter, ok := cc.driverProxy.(imageRejecter)
	if !ok {
		return errors.New("driver proxy does not support reject image")
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	_, err := rejecter.RejectImage(imageCorrelationID, position, reason)
	return err
}
