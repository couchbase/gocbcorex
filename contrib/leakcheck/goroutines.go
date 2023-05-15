package leakcheck

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func ReportLeakedGoroutines() bool {
	// We always expect that only the current goroutine is running.  This assumption
	// is based on the fact that it would not be considered safe to be checking for leaked
	// goroutines when there were concurrent goroutines still running.
	expectedGoroutineCount := 1

	// We allow up to 1 second for goroutines to finish their cleanup.  Since we use
	// Gosched to schedule other goroutines as quickly as possible, anything that takes
	// longer than 1 second implies that it is not 'immediately' cleaning up, and that we
	// likely have a leak.
	goroutineCleanupPeriod := 1 * time.Second

	// Loop for at most a second checking for goroutines leaks, this gives any HTTP goroutines time to shutdown
	var finalGoroutineCount int
	start := time.Now()
	for time.Since(start) <= goroutineCleanupPeriod {
		// Run Gosched to hopefully give closing goroutines time to shut down
		runtime.Gosched()

		// Check if we have the appropriate goroutine count now
		finalGoroutineCount = runtime.NumGoroutine()
		if finalGoroutineCount == expectedGoroutineCount {
			break
		}

		// Sleep for 10ms if not.
		time.Sleep(10 * time.Millisecond)
	}

	if finalGoroutineCount != expectedGoroutineCount {
		log.Printf("Detected a goroutine leak (%d goroutines != %d)", finalGoroutineCount, expectedGoroutineCount)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		return false
	}

	log.Printf("No goroutines appear to have leaked (%d before == %d after)", finalGoroutineCount, expectedGoroutineCount)
	return true
}
