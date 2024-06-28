package transactionsx

import "context"

type Hook interface {
	Before(ctx context.Context) error
	After(ctx context.Context) error
}

type HookWithDocID interface {
	Before(ctx context.Context, docID []byte) error
	After(ctx context.Context, docID []byte) error
}

func invokeHook[TRes any](ctx context.Context, hook Hook, funcFn func() (TRes, error)) (TRes, error) {
	if hook == nil {
		return funcFn()
	}

	err := hook.Before(ctx)
	if err != nil {
		var emptyTRes TRes
		return emptyTRes, err
	}

	res, err := funcFn()
	if err != nil {
		return res, err
	}

	err = hook.After(ctx)
	if err != nil {
		var emptyTRes TRes
		return emptyTRes, err
	}

	return res, err
}

func invokeNoResHook(ctx context.Context, hook Hook, funcFn func() error) error {
	_, err := invokeHook(ctx, hook, func() (struct{}, error) {
		return struct{}{}, funcFn()
	})
	return err
}

func invokeHookWithDocID[TRes any](ctx context.Context, hook HookWithDocID, docID []byte, funcFn func() (TRes, error)) (TRes, error) {
	if hook == nil {
		return funcFn()
	}

	err := hook.Before(ctx, docID)
	if err != nil {
		var emptyTRes TRes
		return emptyTRes, err
	}

	res, err := funcFn()
	if err != nil {
		return res, err
	}

	err = hook.After(ctx, docID)
	if err != nil {
		var emptyTRes TRes
		return emptyTRes, err
	}

	return res, err
}

func invokeNoResHookWithDocID(ctx context.Context, hook HookWithDocID, docID []byte, funcFn func() error) error {
	_, err := invokeHookWithDocID(ctx, hook, docID, func() (struct{}, error) {
		return struct{}{}, funcFn()
	})
	return err
}

// TransactionHooks provides a number of internal hooks.
type TransactionHooks struct {
	ATRCommit                        Hook
	DocCommitted                     HookWithDocID
	RemovingDocDuringStagedInsert    HookWithDocID
	RollbackDeleteInserted           HookWithDocID
	StagedInsert                     HookWithDocID
	StagedRemove                     HookWithDocID
	StagedReplace                    HookWithDocID
	DocRemoved                       HookWithDocID
	DocRolledBack                    HookWithDocID
	StagedReplaceComplete            HookWithDocID
	StagedRemoveComplete             HookWithDocID
	StagedInsertComplete             HookWithDocID
	RollbackReplaceOrRemove          HookWithDocID
	CheckATREntryForBlockingDoc      HookWithDocID
	DocGet                           HookWithDocID
	GetDocInExistsDuringStagedInsert HookWithDocID
	RemoveStagedInsert               HookWithDocID
	DocsCommitted                    Hook
	DocsRemoved                      Hook
	ATRPending                       Hook
	ATRComplete                      Hook
	ATRRolledBack                    Hook
	ATRAborted                       Hook
	ATRCommitAmbiguityResolution     Hook

	AfterGetComplete         func(ctx context.Context, docID []byte) error
	RandomATRIDForVbucket    func(ctx context.Context) (string, error)
	HasExpiredClientSideHook func(ctx context.Context, stage string, docID []byte) bool
}

// TransactionCleanupHooks provides a number of internal hooks used for testing.
type TransactionCleanupHooks struct {
	ATRGet                    HookWithDocID
	DocGet                    HookWithDocID
	RemoveLinks               HookWithDocID
	CommitDoc                 HookWithDocID
	RemoveDocStagedForRemoval HookWithDocID
	RemoveDoc                 HookWithDocID
	ATRRemove                 HookWithDocID
}

// TransactionClientRecordHooks provides a number of internal hooks used for testing.
type TransactionClientRecordHooks struct {
	CreateRecord Hook
	RemoveClient Hook
	UpdateCAS    Hook
	GetRecord    Hook
	UpdateRecord Hook
}

// nolint: deadcode,varcheck
const (
	hookStageRollback           = "rollback"
	hookStageGet                = "get"
	hookStageInsert             = "insert"
	hookStageReplace            = "replace"
	hookStageRemove             = "remove"
	hookStageCommit             = "commit"
	hookStageAbortGetATR        = "abortGetAtr"
	hookStageRollbackDoc        = "rollbackDoc"
	hookStageDeleteInserted     = "deleteInserted"
	hookStageCreateStagedInsert = "createdStagedInsert"
	hookStageRemoveStagedInsert = "removeStagedInsert"
	hookStageRemoveDoc          = "removeDoc"
	hookStageCommitDoc          = "commitDoc"

	hookStageWWC = "writeWriteConflict"

	hookStageATRCommit                    = "atrCommit"
	hookStageATRCommitAmbiguityResolution = "atrCommitAmbiguityResolution"
	hookStageATRAbort                     = "atrAbort"
	hookStageATRRollback                  = "atrRollbackComplete"
	hookStageATRPending                   = "atrPending"
	hookStageATRComplete                  = "atrComplete"
)
