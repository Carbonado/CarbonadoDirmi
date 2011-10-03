/*
 * Copyright 2008-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.repo.dirmi;

import java.rmi.server.Unreferenced;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RemoteTransactionServer extends AbstractQueuedSynchronizer
    implements RemoteTransaction, Unreferenced
{
    private volatile Transaction mTxn;

    RemoteTransactionServer(Transaction txn) {
        mTxn = txn;
    }

    public void commit() throws PersistException {
        Transaction txn = mTxn;
        if (txn != null) {
            txn.commit();
        }
    }

    public void exit() throws PersistException {
        Transaction txn = mTxn;
        if (txn != null) {
            txn.exit();
            // Allow Transaction to be freed before unreferenced is called.
            mTxn = null;
        }
    }

    public void setForUpdate(boolean forUpdate) {
        Transaction txn = mTxn;
        if (txn != null) {
            txn.setForUpdate(forUpdate);
        }
    }

    public boolean isForUpdate() {
        Transaction txn = mTxn;
        return txn == null ? false : txn.isForUpdate();
    }

    public IsolationLevel getIsolationLevel() {
        Transaction txn = mTxn;
        return txn == null ? IsolationLevel.NONE : txn.getIsolationLevel();
    }

    public void unreferenced() {
        try {
            exit();
        } catch (PersistException e) {
            // Ignore.
        }
    }

    /**
     * Acquires an exclusive lock on this object and then attaches the
     * transaction to the current thread. Lock acquisition is not re-entrant.
     */
    void attach() {
        super.acquire(0);

        Transaction txn = mTxn;
        if (txn != null) {
            txn.attach();
        }
    }

    /**
     * Releases exclusive lock and detaches the transaction from the current
     * thread.
     */
    void detach() {
        try {
            Transaction txn = mTxn;
            if (txn != null) {
                txn.detach();
            }
        } finally {
            // Release exclusive lock.
            super.release(0);
        }
    }

    // Methods for AbstractQueuedSynchronizer support.

    @Override
    protected boolean tryAcquire(int arg) {
        // Logic to acquire lock fairly.
        return getState() == 0 && !shouldWait() && compareAndSetState(0, 1);
    }

    @Override
    protected boolean tryRelease(int arg) {
        setState(0);
        return true;
    }

    private boolean shouldWait() {
        return hasQueuedThreads() && getFirstQueuedThread() != Thread.currentThread();
    }
}
