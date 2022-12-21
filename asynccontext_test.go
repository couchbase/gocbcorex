package core

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestAsyncContextCancelNoOps(t *testing.T) {
	ctx := NewAsyncContext()
	ctx.Cancel()
}

func TestAsyncContextCancelOneOp(t *testing.T) {
	numCancelCalls := 0

	ctx := NewAsyncContext()

	cancelCtx := ctx.WithCancellation()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return true
	})

	ctx.Cancel()

	require.Equal(t, 1, numCancelCalls)
}

func TestAsyncContextCancelTwoOp(t *testing.T) {
	numCancelCalls1 := 0
	numCancelCalls2 := 0

	ctx := NewAsyncContext()

	cancelCtx1 := ctx.WithCancellation()
	cancelCtx1.OnCancel(func(err error) bool {
		numCancelCalls1++
		return true
	})

	cancelCtx2 := ctx.WithCancellation()
	cancelCtx2.OnCancel(func(err error) bool {
		numCancelCalls2++
		return true
	})

	ctx.Cancel()

	require.Equal(t, 1, numCancelCalls1)
	require.Equal(t, 1, numCancelCalls2)
}

func TestAsyncContextCreateAfterCancel(t *testing.T) {
	numCancelCalls := 0

	ctx := NewAsyncContext()

	ctx.Cancel()

	cancelCtx := ctx.WithCancellation()
	cancelCtx.OnCancel(func(err error) bool {
		numCancelCalls++
		return true
	})

	require.Equal(t, 1, numCancelCalls)
}

func sliceInsertAt[T any](s []T, pos int, val T) []T {
	if pos == len(s) {
		return append(s, val)
	}
	movedArr := append(s[:pos+1], s[pos:]...)
	movedArr[pos] = val
	return movedArr
}

func generatePermutations() [][]string {
	basePermutations := [][]string{
		{"CC:Create", "CC:MarkComplete", "CC:OnCancelFalse"},
		{"CC:Create", "CC:OnCancelFalse", "CC:MarkComplete"},
		// these are only valid if cancel is actually called...
		{"CC:Create", "CC:OnCancelTrue"},
	}

	// add permutations of the places that cancellation might occur...
	var cancelPermutations [][]string
	for _, basePermutation := range basePermutations {
		// need to copy to avoid affecting the original permutation
		basePermutation := append([]string{}, basePermutation...)

		// mark can occur anywhere
		for markPos := 0; markPos <= len(basePermutation); markPos++ {
			markPermutation := sliceInsertAt(basePermutation, markPos, "C:MarkCanceled")

			for cancelPos := 0; cancelPos <= len(markPermutation); cancelPos++ {
				// need to copy to avoid overwritting eachother
				markPermutation := append([]string{}, markPermutation...)

				cancelPermutation := sliceInsertAt(markPermutation, cancelPos, "CC:InternalCancel")
				cancelPermutations = append(cancelPermutations, cancelPermutation)
			}
		}
	}

	permutations := append([][]string{}, basePermutations...)
	permutations = append(permutations, cancelPermutations...)

	var finalPermutations [][]string
	for _, permutation := range permutations {
		createPos := slices.Index(permutation, "CC:Create")
		onCancelTruePos := slices.Index(permutation, "CC:OnCancelTrue")
		markCanceledPos := slices.Index(permutation, "C:MarkCanceled")
		internalCancelPos := slices.Index(permutation, "CC:InternalCancel")
		markCompletePos := slices.Index(permutation, "CC:MarkComplete")

		// OnCancelTrue is only valid if this permutation includes cancellation
		if onCancelTruePos != -1 && markCanceledPos == -1 {
			continue
		}

		// If we MarkCanceled before Create, InternalCancel wont ever be called
		if markCanceledPos != -1 && createPos != -1 {
			if markCanceledPos < createPos && internalCancelPos != -1 {
				continue
			}
		}

		// InternalCancel must always be after MarkCanceled
		if internalCancelPos != -1 && markCanceledPos != -1 {
			if internalCancelPos < markCanceledPos {
				continue
			}
		}

		// Any case where MarkComplete comes before MarkCanceled is invalid since
		// the MarkComplete will have removed the cancel context from the parent.
		if markCompletePos != -1 && markCanceledPos != -1 {
			if markCompletePos < markCanceledPos {
				continue
			}
		}

		finalPermutations = append(finalPermutations, permutation)
	}

	return finalPermutations
}

func generatePermutationName(permutation []string) string {
	permKeys := []string{}
	for _, permAct := range permutation {
		switch permAct {
		case "CC:Create":
			permKeys = append(permKeys, "C")
		case "CC:MarkComplete":
			permKeys = append(permKeys, "MC")
		case "CC:OnCancelTrue":
			permKeys = append(permKeys, "OCT")
		case "CC:OnCancelFalse":
			permKeys = append(permKeys, "OCF")
		case "C:MarkCanceled":
			permKeys = append(permKeys, "MX")
		case "CC:InternalCancel":
			permKeys = append(permKeys, "IC")
		default:
			panic("unexpected permutation action '" + permAct + "'")
		}
	}
	return strings.Join(permKeys, "_")
}

func TestAsyncContextPermutations(t *testing.T) {
	permutations := generatePermutations()

	for _, permutation := range permutations {
		permutationName := generatePermutationName(permutation)
		t.Run(permutationName, func(t *testing.T) {
			ctx := NewAsyncContext()
			var cancelCtx *AsyncCancelContext
			var shouldRun bool
			var numCancelCalls int

			for _, permAct := range permutation {
				switch permAct {
				case "CC:Create":
					cancelCtx = ctx.WithCancellation()
				case "CC:MarkComplete":
					shouldRun = cancelCtx.MarkComplete()
				case "CC:OnCancelTrue":
					cancelCtx.OnCancel(func(err error) bool {
						numCancelCalls++
						return true
					})
				case "CC:OnCancelFalse":
					cancelCtx.OnCancel(func(err error) bool {
						numCancelCalls++
						return false
					})
				case "C:MarkCanceled":
					ctx.markCanceled(context.Canceled)
				case "CC:InternalCancel":
					cancelCtx.internalCancel(context.Canceled)
				default:
					panic("unexpected permutation action '" + permAct + "'")
				}
			}

			if shouldRun {
				require.Equal(t, 0, numCancelCalls)
			} else {
				require.Equal(t, 1, numCancelCalls)
			}
		})
	}
}
