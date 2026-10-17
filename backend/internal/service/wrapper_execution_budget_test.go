package service

import (
	"testing"
	"time"
)

func TestWrapperJSONExecutionBudget(t *testing.T) {
	for _, value := range []string{"", "invalid", "-1", "3601"} {
		t.Setenv("SUB2API_WRAPPER_JSON_TIMEOUT_SECONDS", value)
		if WrapperJSONExecutionBudget() != 31*time.Minute {
			t.Fatal(value)
		}
	}
	t.Setenv("SUB2API_WRAPPER_JSON_TIMEOUT_SECONDS", "1800")
	if WrapperJSONExecutionBudget() != 30*time.Minute {
		t.Fatal("override ignored")
	}
}
