package testutilsint

import (
	"testing"

	"golang.org/x/exp/slices"
)

type TestFeature string

const (
	TestFeatureRangeScan    TestFeature = "range-scan"
	TestFeatureScopedSearch TestFeature = "scoped-search"
	TestFeatureAnalytics    TestFeature = "analytics"
)

var AllTestFeatures = []TestFeature{
	TestFeatureRangeScan,
	TestFeatureScopedSearch,
	TestFeatureAnalytics,
}

func SupportsFeature(feat TestFeature) bool {
	return slices.Contains(TestOpts.SupportedFeatures, feat)
}

func SkipIfUnsupportedFeature(t *testing.T, feat TestFeature) {
	if !SupportsFeature(feat) {
		t.Skipf("skipping unsupported feature (%s)", feat)
	}
}
