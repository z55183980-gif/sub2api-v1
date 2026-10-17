package service

import (
	"os"
	"strconv"
	"time"
)

// Production CLI execution is bounded at 30 minutes. Leave one minute for
// process teardown, terminal validation and durable result persistence.
func WrapperJSONExecutionBudget() time.Duration {
	seconds, err := strconv.Atoi(os.Getenv("SUB2API_WRAPPER_JSON_TIMEOUT_SECONDS"))
	if err != nil || seconds < 60 || seconds > 3600 {
		seconds = 31 * 60
	}
	return time.Duration(seconds) * time.Second
}
