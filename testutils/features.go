package testutils

import (
	"testing"

	"golang.org/x/exp/slices"
)

type TestFeature string

const (
	TestFeatureRangeScan TestFeature = "range-scan"
)

var AllTestFeatures = []TestFeature{
	TestFeatureRangeScan,
}

func SupportsFeature(feat TestFeature) bool {
	return slices.Contains(TestOpts.SupportedFeatures, feat)
}

func SkipIfUnsupportedFeature(t *testing.T, feat TestFeature) {
	if !SupportsFeature(feat) {
		t.Skipf("skipping unsupported feature (%s)", feat)
	}
}
