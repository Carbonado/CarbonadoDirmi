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

import org.cojen.dirmi.util.Wrapper;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.spi.AbstractRepository;

import com.amazon.carbonado.txn.TransactionManager;
import com.amazon.carbonado.txn.TransactionScope;

/**
 * Creates a client repository from a {@link RemoteRepository}, as served by
 * {@link RemoteRepositoryServer}.
 *
 * @author Brian S O'Neill
 */
public class ClientRepository extends AbstractRepository<RemoteTransaction> {
    /**
     * Returns client access to a remote repository server.
     */
    public static Repository from(RemoteRepository remote) throws RepositoryException {
        return new ClientRepository(remote.getName(), remote);
    }

    private final RemoteRepository mRepository;
    private final TransactionManager<RemoteTransaction> mTxnMgr;

    private ClientRepository(String name, RemoteRepository remote) {
        super(name);
        mRepository = remote;
        mTxnMgr = new ClientTransactionManager(remote);
    }

    @Override
    protected org.apache.commons.logging.Log getLog() {
        return null;
    }

    @Override
    protected <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws RepositoryException
    {
        return new ClientStorage<S>(type, this, mRepository.storageFor(type));
    }

    @Override
    protected SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException
    {
        RemoteSequenceValueProducer producer = mRepository.getSequenceValueProducer(name);
        return Wrapper
            .from(SequenceValueProducer.class, RemoteSequenceValueProducer.class)
            .wrap(producer);
    }

    @Override
    protected final TransactionManager<RemoteTransaction> transactionManager() {
        return mTxnMgr;
    }

    @Override
    protected final TransactionScope<RemoteTransaction> localTransactionScope() {
        return mTxnMgr.localScope();
    }
}
