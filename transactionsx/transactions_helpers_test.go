package gocbcore

func testBlkGet(txn *Transaction, opts TransactionGetOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Get(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkInsert(txn *Transaction, opts TransactionInsertOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Insert(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkReplace(txn *Transaction, opts TransactionReplaceOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Replace(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkRemove(txn *Transaction, opts TransactionRemoveOptions) (resOut *TransactionGetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Remove(opts, func(res *TransactionGetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkCommit(txn *Transaction) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Commit(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkRollback(txn *Transaction) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Rollback(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkSerialize(txn *Transaction) (txnBytesOut []byte, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.SerializeAttempt(func(txnBytes []byte, err error) {
		txnBytesOut = txnBytes
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}
