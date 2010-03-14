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

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.TimeUnit;

import java.rmi.Remote;

import org.cojen.dirmi.util.Wrapper;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.layout.Layout;

import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueProducer;

/**
 * Wraps a repository for server-side access, which can be accessed on the
 * client by {@link ClientRepository}.
 *
 * @author Brian S O'Neill
 */
public class RemoteRepositoryServer implements RemoteRepository {
    /**
     * Returns a remotely servable repository.
     */
    public static RemoteRepository from(Repository repo) {
        return new RemoteRepositoryServer(repo);
    }

    private final Repository mRepository;
    private Map<StorableLayoutKey, RemoteStorage> mStorageMap;

    private RemoteRepositoryServer(Repository repo) {
        mRepository = repo;
        mStorageMap = new HashMap<StorableLayoutKey, RemoteStorage>();
    }

    public String getName() {
        return mRepository.getName();
    }

    public RemoteStorageTransport storageFor(StorableTypeTransport transport)
        throws RepositoryException
    {
        Class storableType = transport.getStorableType();
        Layout clientLayout = transport.getLayout();
        StorableLayoutKey key = new StorableLayoutKey(storableType, clientLayout);

        RemoteStorage remoteStorage;
        synchronized (mStorageMap) {
            remoteStorage = mStorageMap.get(key);
            if (remoteStorage == null) {
                Storage storage = mRepository.storageFor(storableType);
                StorableWriter writer =
                    ReconstructedCache.THE.writerFor(storableType, clientLayout);
                remoteStorage = new RemoteStorageServer(storage, writer);
                mStorageMap.put(key, remoteStorage);
            }
        }

        Layout localLayout = ReconstructedCache.THE.layoutFor(storableType);
        return new RemoteStorageTransport(storableType, localLayout, remoteStorage);
    }

    public RemoteTransaction enterTransaction(RemoteTransaction parent, IsolationLevel level) {
        if (!attach(parent)) {
            return new FailedTransaction();
        }
        try {
            Transaction txn = mRepository.enterTransaction(level);
            txn.detach();
            return new RemoteTransactionServer(txn);
        } finally {
            detach(parent);
        }
    }

    public RemoteTransaction enterTransaction(RemoteTransaction parent, IsolationLevel level,
                                              int timeout, TimeUnit unit)
    {
        if (!attach(parent)) {
            return new FailedTransaction();
        }
        try {
            Transaction txn = mRepository.enterTransaction(level);
            txn.setDesiredLockTimeout(timeout, unit);
            txn.detach();
            return new RemoteTransactionServer(txn);
        } finally {
            detach(parent);
        }
    }

    public RemoteTransaction enterTopTransaction(IsolationLevel level) {
        Transaction txn = mRepository.enterTopTransaction(level);
        txn.detach();
        return new RemoteTransactionServer(txn);
    }

    public RemoteTransaction enterTopTransaction(IsolationLevel level,
                                                 int timeout, TimeUnit unit)
    {
        Transaction txn = mRepository.enterTopTransaction(level);
        txn.setDesiredLockTimeout(timeout, unit);
        txn.detach();
        return new RemoteTransactionServer(txn);
    }

    public RemoteSequenceValueProducer getSequenceValueProducer(String name)
        throws RepositoryException
    {
        SequenceCapability cap = mRepository.getCapability(SequenceCapability.class);
        if (cap == null) {
            throw new RepositoryException("Sequences not supported");
        }
        SequenceValueProducer producer = cap.getSequenceValueProducer(name);
        return Wrapper
            .from(RemoteSequenceValueProducer.class, SequenceValueProducer.class)
            .wrap(producer);
    }

    private boolean attach(RemoteTransaction parent) {
        if (parent != null) {
            try {
                ((RemoteTransactionServer) parent).attach();
            } catch (ClassCastException e) {
                // This means that the parent transaction has been disconnected
                // and this transaction has nothing to attach to. The client
                // needs to be notified that it has an invalid transaction.
                return false;
            }
        }
        return true;
    }

    private void detach(RemoteTransaction parent) {
        if (parent != null) {
            ((RemoteTransactionServer) parent).detach();
        }
    }
}
