package leakcheck

func EnableAll() {
	EnableHttpResponseTracking()
	PrecheckGoroutines()
}

func ReportAll() bool {
	httpCheck := ReportLeakedHttpResponses()
	goroutinesCheck := ReportLeakedGoroutines()
	return httpCheck && goroutinesCheck
}
