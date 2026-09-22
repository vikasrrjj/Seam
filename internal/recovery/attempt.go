package recovery

import (
	"fmt"
	"strconv"
	"strings"
)

// parseAttempt splits an attempt id such as "gen:0:attempt:3" into its numeric
// generation and attempt components.
func parseAttempt(attempt string) (genNum int, attemptNum int, err error) {
	parts := strings.Split(attempt, ":")
	if len(parts) != 4 || parts[0] != "gen" || parts[2] != "attempt" {
		return 0, 0, fmt.Errorf("invalid attempt %q", attempt)
	}
	genNum, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid generation number in %q: %w", attempt, err)
	}
	attemptNum, err = strconv.Atoi(parts[3])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid attempt number in %q: %w", attempt, err)
	}
	return genNum, attemptNum, nil
}

// BumpAttempt returns the next attempt id within the same generation.
func BumpAttempt(attempt string) (string, error) {
	genNum, attemptNum, err := parseAttempt(attempt)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("gen:%d:attempt:%d", genNum, attemptNum+1), nil
}

// BumpGeneration returns a fresh generation id with attempt reset to 0.
func BumpGeneration(attempt string) (string, error) {
	genNum, _, err := parseAttempt(attempt)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("gen:%d:attempt:%d", genNum+1, 0), nil
}

// FormatAttempt builds an attempt id from numeric components.
func FormatAttempt(genNum, attemptNum int) string {
	return fmt.Sprintf("gen:%d:attempt:%d", genNum, attemptNum)
}
