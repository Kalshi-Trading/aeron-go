package aeron

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorCodeValuesMatchJava(t *testing.T) {
	expected := map[ErrorCode]int32{
		ErrorCodeUnused: 0, ErrorCodeInvalidChannel: 1, ErrorCodeUnknownSubscription: 2,
		ErrorCodeUnknownPublication: 3, ErrorCodeChannelEndpointError: 4, ErrorCodeUnknownCounter: 5,
		ErrorCodeUnknownCommandTypeID: 6, ErrorCodeMalformedCommand: 7, ErrorCodeNotSupported: 8,
		ErrorCodeUnknownHost: 9, ErrorCodeResourceTemporarilyUnavailable: 10, ErrorCodeGenericError: 11,
		ErrorCodeStorageSpace: 12, ErrorCodeImageRejected: 13, ErrorCodePublicationRevoked: 14,
		ErrorCodeUnknownCodeValue: -1,
	}
	for code, v := range expected {
		if code.Value() != v {
			t.Errorf("%s value %d != %d", code, code.Value(), v)
		}
		if v >= 0 && ErrorCodeFromValue(v) != code {
			t.Errorf("ErrorCodeFromValue(%d) = %s", v, ErrorCodeFromValue(v))
		}
	}
	if ErrorCodeFromValue(99) != ErrorCodeUnknownCodeValue || ErrorCodeFromValue(-7) != ErrorCodeUnknownCodeValue {
		t.Error("unknown values must map to ErrorCodeUnknownCodeValue")
	}
	if ErrorCodeInvalidChannel.String() != "INVALID_CHANNEL" {
		t.Errorf("unexpected name %q", ErrorCodeInvalidChannel.String())
	}
}

func TestRegistrationErrorAs(t *testing.T) {
	err := fmt.Errorf("adding: %w", NewRegistrationError(42, 1, "ERROR - invalid channel"))

	var reg *RegistrationError
	if !errors.As(err, &reg) {
		t.Fatal("errors.As RegistrationError failed")
	}
	if reg.CorrelationID != 42 || reg.ErrorCode() != ErrorCodeInvalidChannel || reg.ErrorCodeValue != 1 {
		t.Errorf("unexpected fields %+v", reg)
	}
	var ae *AeronError
	if !errors.As(err, &ae) || ae.Category != CategoryError {
		t.Errorf("errors.As AeronError failed: %v", ae)
	}
	if c, ok := CategoryOf(err); !ok || c != CategoryError {
		t.Errorf("CategoryOf = %v %v", c, ok)
	}
	if reg.Error() != "ERROR - invalid channel, errorCodeValue=1" {
		t.Errorf("message %q", reg.Error())
	}
	var to *TimeoutError
	if errors.As(err, &to) {
		t.Error("registration error must not be a timeout")
	}

	warn := NewRegistrationError(1, int32(ErrorCodeResourceTemporarilyUnavailable), "busy")
	if c, _ := CategoryOf(warn); c != CategoryWarn {
		t.Errorf("RESOURCE_TEMPORARILY_UNAVAILABLE should be WARN, got %s", c)
	}
}

func TestTimeoutErrorsAs(t *testing.T) {
	for _, err := range []error{
		NewDriverTimeoutError("driver"),
		NewClientTimeoutError("client"),
		NewConductorServiceTimeoutError("service"),
	} {
		wrapped := fmt.Errorf("wrap: %w", err)
		var to *TimeoutError
		if !errors.As(wrapped, &to) {
			t.Errorf("%T not a TimeoutError", err)
		}
		var ae *AeronError
		if !errors.As(wrapped, &ae) {
			t.Errorf("%T not an AeronError", err)
		}
		if !IsFatal(wrapped) {
			t.Errorf("%T should be fatal", err)
		}
	}
	var dte *DriverTimeoutError
	if !errors.As(fmt.Errorf("x: %w", NewDriverTimeoutError("d")), &dte) {
		t.Error("DriverTimeoutError not extracted")
	}
	var cte *ClientTimeoutError
	if errors.As(NewDriverTimeoutError("d"), &cte) {
		t.Error("driver timeout must not match ClientTimeoutError")
	}
}

func TestChannelEndpointErrorAs(t *testing.T) {
	err := fmt.Errorf("x: %w", NewChannelEndpointError(7, "Address already in use"))
	var ce *ChannelEndpointError
	if !errors.As(err, &ce) || ce.StatusIndicatorID != 7 {
		t.Fatalf("errors.As ChannelEndpointError failed: %v", ce)
	}
	if IsFatal(err) {
		t.Error("channel endpoint error is not fatal")
	}
	if _, ok := CategoryOf(errors.New("plain")); ok {
		t.Error("plain errors have no category")
	}
}

func TestCategoryStrings(t *testing.T) {
	if CategoryFatal.String() != "FATAL" || CategoryError.String() != "ERROR" || CategoryWarn.String() != "WARN" {
		t.Error("category names must match Java")
	}
	if NewAeronEvent("hello").Error() != "WARN - hello" {
		t.Errorf("event %q", NewAeronEvent("hello").Error())
	}
}
