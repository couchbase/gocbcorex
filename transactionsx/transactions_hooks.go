package transactionsx

import "context"

// TransactionHooks provides a number of internal hooks.
type TransactionHooks interface {
	BeforeATRCommit(ctx context.Context) error
	AfterATRCommit(ctx context.Context) error
	BeforeDocCommitted(ctx context.Context, docID []byte) error
	BeforeRemovingDocDuringStagedInsert(ctx context.Context, docID []byte) error
	BeforeRollbackDeleteInserted(ctx context.Context, docID []byte) error
	AfterDocCommittedBeforeSavingCAS(ctx context.Context, docID []byte) error
	AfterDocCommitted(ctx context.Context, docID []byte) error
	BeforeStagedInsert(ctx context.Context, docID []byte) error
	BeforeStagedRemove(ctx context.Context, docID []byte) error
	BeforeStagedReplace(ctx context.Context, docID []byte) error
	BeforeDocRemoved(ctx context.Context, docID []byte) error
	BeforeDocRolledBack(ctx context.Context, docID []byte) error
	AfterDocRemovedPreRetry(ctx context.Context, docID []byte) error
	AfterDocRemovedPostRetry(ctx context.Context, docID []byte) error
	AfterGetComplete(ctx context.Context, docID []byte) error
	AfterStagedReplaceComplete(ctx context.Context, docID []byte) error
	AfterStagedRemoveComplete(ctx context.Context, docID []byte) error
	AfterStagedInsertComplete(ctx context.Context, docID []byte) error
	AfterRollbackReplaceOrRemove(ctx context.Context, docID []byte) error
	AfterRollbackDeleteInserted(ctx context.Context, docID []byte) error
	BeforeCheckATREntryForBlockingDoc(ctx context.Context, docID []byte) error
	BeforeDocGet(ctx context.Context, docID []byte) error
	BeforeGetDocInExistsDuringStagedInsert(ctx context.Context, docID []byte) error
	BeforeRemoveStagedInsert(ctx context.Context, docID []byte) error
	AfterRemoveStagedInsert(ctx context.Context, docID []byte) error
	AfterDocsCommitted(ctx context.Context) error
	AfterDocsRemoved(ctx context.Context) error
	AfterATRPending(ctx context.Context) error
	BeforeATRPending(ctx context.Context) error
	BeforeATRComplete(ctx context.Context) error
	BeforeATRRolledBack(ctx context.Context) error
	AfterATRComplete(ctx context.Context) error
	BeforeATRAborted(ctx context.Context) error
	AfterATRAborted(ctx context.Context) error
	AfterATRRolledBack(ctx context.Context) error
	BeforeATRCommitAmbiguityResolution(ctx context.Context) error
	RandomATRIDForVbucket(ctx context.Context) (string, error)
	HasExpiredClientSideHook(ctx context.Context, stage string, docID []byte) (bool, error)
}

// TransactionCleanUpHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type TransactionCleanUpHooks interface {
	BeforeATRGet(ctx context.Context, id []byte) error
	BeforeDocGet(ctx context.Context, id []byte) error
	BeforeRemoveLinks(ctx context.Context, id []byte) error
	BeforeCommitDoc(ctx context.Context, id []byte) error
	BeforeRemoveDocStagedForRemoval(ctx context.Context, id []byte) error
	BeforeRemoveDoc(ctx context.Context, id []byte) error
	BeforeATRRemove(ctx context.Context, id []byte) error
}

// TransactionClientRecordHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type TransactionClientRecordHooks interface {
	BeforeCreateRecord(ctx context.Context) error
	BeforeRemoveClient(ctx context.Context) error
	BeforeUpdateCAS(ctx context.Context) error
	BeforeGetRecord(ctx context.Context) error
	BeforeUpdateRecord(ctx context.Context) error
}

// TransactionDefaultHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type TransactionDefaultHooks struct {
}

// BeforeATRCommit occurs before an ATR is committed.
func (dh *TransactionDefaultHooks) BeforeATRCommit(ctx context.Context) error {
	return nil
}

// AfterATRCommit occurs after an ATR is committed.
func (dh *TransactionDefaultHooks) AfterATRCommit(ctx context.Context) error {
	return nil
}

// BeforeDocCommitted occurs before a document is committed.
func (dh *TransactionDefaultHooks) BeforeDocCommitted(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeRemovingDocDuringStagedInsert occurs before removing a document during staged insert.
func (dh *TransactionDefaultHooks) BeforeRemovingDocDuringStagedInsert(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeRollbackDeleteInserted occurs before rolling back a delete.
func (dh *TransactionDefaultHooks) BeforeRollbackDeleteInserted(ctx context.Context, docID []byte) error {
	return nil
}

// AfterDocCommittedBeforeSavingCAS occurs after committed a document before saving the CAS.
func (dh *TransactionDefaultHooks) AfterDocCommittedBeforeSavingCAS(ctx context.Context, docID []byte) error {
	return nil
}

// AfterDocCommitted occurs after a document is committed.
func (dh *TransactionDefaultHooks) AfterDocCommitted(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeStagedInsert occurs before staging an insert.
func (dh *TransactionDefaultHooks) BeforeStagedInsert(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeStagedRemove occurs before staging a remove.
func (dh *TransactionDefaultHooks) BeforeStagedRemove(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeStagedReplace occurs before staging a replace.
func (dh *TransactionDefaultHooks) BeforeStagedReplace(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeDocRemoved occurs before removing a document.
func (dh *TransactionDefaultHooks) BeforeDocRemoved(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeDocRolledBack occurs before a document is rolled back.
func (dh *TransactionDefaultHooks) BeforeDocRolledBack(ctx context.Context, docID []byte) error {
	return nil
}

// AfterDocRemovedPreRetry occurs after removing a document before retry.
func (dh *TransactionDefaultHooks) AfterDocRemovedPreRetry(ctx context.Context, docID []byte) error {
	return nil
}

// AfterDocRemovedPostRetry occurs after removing a document after retry.
func (dh *TransactionDefaultHooks) AfterDocRemovedPostRetry(ctx context.Context, docID []byte) error {
	return nil
}

// AfterGetComplete occurs after a get completes.
func (dh *TransactionDefaultHooks) AfterGetComplete(ctx context.Context, docID []byte) error {
	return nil
}

// AfterStagedReplaceComplete occurs after staging a replace is completed.
func (dh *TransactionDefaultHooks) AfterStagedReplaceComplete(ctx context.Context, docID []byte) error {
	return nil
}

// AfterStagedRemoveComplete occurs after staging a remove is completed.
func (dh *TransactionDefaultHooks) AfterStagedRemoveComplete(ctx context.Context, docID []byte) error {
	return nil
}

// AfterStagedInsertComplete occurs after staging an insert is completed.
func (dh *TransactionDefaultHooks) AfterStagedInsertComplete(ctx context.Context, docID []byte) error {
	return nil
}

// AfterRollbackReplaceOrRemove occurs after rolling back a replace or remove.
func (dh *TransactionDefaultHooks) AfterRollbackReplaceOrRemove(ctx context.Context, docID []byte) error {
	return nil
}

// AfterRollbackDeleteInserted occurs after rolling back a delete.
func (dh *TransactionDefaultHooks) AfterRollbackDeleteInserted(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeCheckATREntryForBlockingDoc occurs before checking the ATR of a blocking document.
func (dh *TransactionDefaultHooks) BeforeCheckATREntryForBlockingDoc(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeDocGet occurs before a document is fetched.
func (dh *TransactionDefaultHooks) BeforeDocGet(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeGetDocInExistsDuringStagedInsert occurs before getting a document for an insert.
func (dh *TransactionDefaultHooks) BeforeGetDocInExistsDuringStagedInsert(ctx context.Context, docID []byte) error {
	return nil
}

// BeforeRemoveStagedInsert occurs before removing a staged insert.
func (dh *TransactionDefaultHooks) BeforeRemoveStagedInsert(ctx context.Context, docID []byte) error {
	return nil
}

// AfterRemoveStagedInsert occurs after removing a staged insert.
func (dh *TransactionDefaultHooks) AfterRemoveStagedInsert(ctx context.Context, docID []byte) error {
	return nil
}

// AfterDocsCommitted occurs after all documents are committed.
func (dh *TransactionDefaultHooks) AfterDocsCommitted(ctx context.Context) error {
	return nil
}

// AfterDocsRemoved occurs after all documents are removed.
func (dh *TransactionDefaultHooks) AfterDocsRemoved(ctx context.Context) error {
	return nil
}

// AfterATRPending occurs after the ATR transitions to pending.
func (dh *TransactionDefaultHooks) AfterATRPending(ctx context.Context) error {
	return nil
}

// BeforeATRPending occurs before the ATR transitions to pending.
func (dh *TransactionDefaultHooks) BeforeATRPending(ctx context.Context) error {
	return nil
}

// BeforeATRComplete occurs before the ATR transitions to complete.
func (dh *TransactionDefaultHooks) BeforeATRComplete(ctx context.Context) error {
	return nil
}

// BeforeATRRolledBack occurs before the ATR transitions to rolled back.
func (dh *TransactionDefaultHooks) BeforeATRRolledBack(ctx context.Context) error {
	return nil
}

// AfterATRComplete occurs after the ATR transitions to complete.
func (dh *TransactionDefaultHooks) AfterATRComplete(ctx context.Context) error {
	return nil
}

// BeforeATRAborted occurs before the ATR transitions to aborted.
func (dh *TransactionDefaultHooks) BeforeATRAborted(ctx context.Context) error {
	return nil
}

// AfterATRAborted occurs after the ATR transitions to aborted.
func (dh *TransactionDefaultHooks) AfterATRAborted(ctx context.Context) error {
	return nil
}

// AfterATRRolledBack occurs after the ATR transitions to rolled back.
func (dh *TransactionDefaultHooks) AfterATRRolledBack(ctx context.Context) error {
	return nil
}

// BeforeATRCommitAmbiguityResolution occurs before ATR commit ambiguity resolution.
func (dh *TransactionDefaultHooks) BeforeATRCommitAmbiguityResolution(ctx context.Context) error {
	return nil
}

// RandomATRIDForVbucket generates a random ATRID for a vbucket.
func (dh *TransactionDefaultHooks) RandomATRIDForVbucket(ctx context.Context) (string, error) {
	return "", nil
}

// HasExpiredClientSideHook checks if a transaction has expired.
func (dh *TransactionDefaultHooks) HasExpiredClientSideHook(ctx context.Context, stage string, docID []byte) (bool, error) {
	return false, nil
}

// TransactionDefaultCleanupHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type TransactionDefaultCleanupHooks struct {
}

// BeforeATRGet happens before an ATR get.
func (dh *TransactionDefaultCleanupHooks) BeforeATRGet(ctx context.Context, id []byte) error {
	return nil
}

// BeforeDocGet happens before an doc get.
func (dh *TransactionDefaultCleanupHooks) BeforeDocGet(ctx context.Context, id []byte) error {
	return nil
}

// BeforeRemoveLinks happens before we remove links.
func (dh *TransactionDefaultCleanupHooks) BeforeRemoveLinks(ctx context.Context, id []byte) error {
	return nil
}

// BeforeCommitDoc happens before we commit a document.
func (dh *TransactionDefaultCleanupHooks) BeforeCommitDoc(ctx context.Context, id []byte) error {
	return nil
}

// BeforeRemoveDocStagedForRemoval happens before we remove a staged document.
func (dh *TransactionDefaultCleanupHooks) BeforeRemoveDocStagedForRemoval(ctx context.Context, id []byte) error {
	return nil
}

// BeforeRemoveDoc happens before we remove a document.
func (dh *TransactionDefaultCleanupHooks) BeforeRemoveDoc(ctx context.Context, id []byte) error {
	return nil
}

// BeforeATRRemove happens before we remove an ATR.
func (dh *TransactionDefaultCleanupHooks) BeforeATRRemove(ctx context.Context, id []byte) error {
	return nil
}

// TransactionDefaultClientRecordHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type TransactionDefaultClientRecordHooks struct {
}

// BeforeCreateRecord happens before we create a cleanup client record.
func (dh *TransactionDefaultClientRecordHooks) BeforeCreateRecord(ctx context.Context) error {
	return nil
}

// BeforeRemoveClient happens before we remove a cleanup client record.
func (dh *TransactionDefaultClientRecordHooks) BeforeRemoveClient(ctx context.Context) error {
	return nil
}

// BeforeUpdateCAS happens before we update a CAS.
func (dh *TransactionDefaultClientRecordHooks) BeforeUpdateCAS(ctx context.Context) error {
	return nil
}

// BeforeGetRecord happens before we get a cleanup client record.
func (dh *TransactionDefaultClientRecordHooks) BeforeGetRecord(ctx context.Context) error {
	return nil
}

// BeforeUpdateRecord happens before we update a cleanup client record.
func (dh *TransactionDefaultClientRecordHooks) BeforeUpdateRecord(ctx context.Context) error {
	return nil
}

// nolint: deadcode,varcheck
const (
	hookRollback           = "rollback"
	hookGet                = "get"
	hookInsert             = "insert"
	hookReplace            = "replace"
	hookRemove             = "remove"
	hookCommit             = "commit"
	hookAbortGetATR        = "abortGetAtr"
	hookRollbackDoc        = "rollbackDoc"
	hookDeleteInserted     = "deleteInserted"
	hookCreateStagedInsert = "createdStagedInsert"
	hookRemoveStagedInsert = "removeStagedInsert"
	hookRemoveDoc          = "removeDoc"
	hookCommitDoc          = "commitDoc"

	hookWWC = "writeWriteConflict"

	hookATRCommit                    = "atrCommit"
	hookATRCommitAmbiguityResolution = "atrCommitAmbiguityResolution"
	hookATRAbort                     = "atrAbort"
	hookATRRollback                  = "atrRollbackComplete"
	hookATRPending                   = "atrPending"
	hookATRComplete                  = "atrComplete"
)
