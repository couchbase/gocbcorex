package leakcheck

func EnableAll(expectedStartGoroutines ...int) {
	EnableHttpResponseTracking()
	PrecheckGoroutines(expectedStartGoroutines...)
}

func ReportAll(expectedEndGoroutines ...int) bool {
	httpCheck := ReportLeakedHttpResponses()
	goroutinesCheck := ReportLeakedGoroutines(expectedEndGoroutines...)
	return httpCheck && goroutinesCheck
}
