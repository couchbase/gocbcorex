package leakcheck

func EnableAll() {
	EnableHttpResponseTracking()
	PrecheckGoroutines()
}

func ReportAll() bool {
	testsPassed := true
	if !ReportLeakedHttpResponses() {
		testsPassed = false
	}
	if !ReportLeakedGoroutines() {
		testsPassed = false
	}
	return testsPassed
}
