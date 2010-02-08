/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.txn.TransactionManager;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ClientTransactionManager extends TransactionManager<RemoteTransaction> {
    private final ClientRepository mRepository;

    ClientTransactionManager(ClientRepository repo) {
        if (repo == null) {
            throw new IllegalArgumentException();
        }
        mRepository = repo;
    }

    protected IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel level) {
        if (level == null) {
            if (parent == null) {
                level = IsolationLevel.READ_COMMITTED;
            } else {
                level = parent.getIsolationLevel();
            }
        }
        return level;
    }

    protected boolean supportsForUpdate() {
        // FIXME: ask remote repository once and cache
        return true;
    }

    protected RemoteTransaction createTxn(RemoteTransaction parent, IsolationLevel level) {
        if (parent == null) {
            return mRepository.getRemoteRepository().enterTopTransaction(level);
        } else {
            return mRepository.getRemoteRepository().enterTransaction(parent, level);
        }
    }

    protected boolean commitTxn(RemoteTransaction txn) throws PersistException {
        txn.commit();
        return true;
    }

    protected void abortTxn(RemoteTransaction txn) throws PersistException {
        txn.exit();
    }
}
