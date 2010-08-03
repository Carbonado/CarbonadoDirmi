/*
 * Copyright 2010 Amazon Technologies, Inc. or its affiliates.
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

import java.io.IOException;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.capability.RemoteProcedure;

import com.amazon.carbonado.util.AbstractPool;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RemoteProcedureExecutorServer implements RemoteProcedureExecutor, ProcedureOpCodes {
    final RemoteRepositoryServer mRepositoryServer;
    private final RemoteStorageRequestor mStorageRequestor;

    private final WriterPool mStorableWriters;

    RemoteProcedureExecutorServer(RemoteRepositoryServer repo, RemoteStorageRequestor r) {
        mRepositoryServer = repo;
        mStorageRequestor = r;
        mStorableWriters = new WriterPool();
    }

    public Pipe remoteCall(RemoteTransaction txn, RemoteProcedure proc, Pipe pipe) {
        if (!mRepositoryServer.attach(txn)) {
            try {
                try {
                    pipe.writeByte(OP_THROWABLE);
                    pipe.writeThrowable
                        (new RepositoryException("Transaction is invalid due to reconnect"));
                } finally {
                    pipe.close();
                }
            } catch (IOException e) {
                // Ignore.
            }
            return null;
        }

        ProcedureRequest request = new ProcedureRequest(this, pipe, txn);

        try {
            try {
                if (proc.handleRequest(mRepositoryServer.mRepository, request)) {
                    request.silentFinish();
                }
            } catch (RepositoryException e) {
                request.silentFinish(e);
            }
        } catch (Exception e) {
            mRepositoryServer.detach(txn);
            Thread t = Thread.currentThread();
            t.getUncaughtExceptionHandler().uncaughtException(t, e);
        }

        return null;
    }

    <S extends Storable> StorableWriter<S> writerFor(Class<S> type) throws RepositoryException {
        return (StorableWriter<S>) mStorableWriters.get(type);
    }

    private class WriterPool
        extends AbstractPool<Class, StorableWriter, RepositoryException> 
    {
        protected StorableWriter create(Class type) throws RepositoryException {
            return ((RemoteStorageServer) mStorageRequestor.serverStorageFor(type))
                .storableWriter();
        }
    }
}
