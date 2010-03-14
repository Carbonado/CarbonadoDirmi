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

import java.rmi.Remote;

import org.cojen.dirmi.Asynchronous;
import org.cojen.dirmi.Batched;
import org.cojen.dirmi.RemoteFailure;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;

/**
 * Remote transaction definition which is RMI compliant but does not actually
 * extend the Transaction interface. Client class is responsible for adapting
 * Transaction calls to RemoteTransaction calls.
 *
 * @author Brian S O'Neill
 */
public interface RemoteTransaction extends Remote {
    @RemoteFailure(exception=PersistException.class)
    void commit() throws PersistException;

    @Asynchronous
    @RemoteFailure(exception=PersistException.class)
    void exit() throws PersistException;

    @Batched
    @RemoteFailure(exception=RepositoryException.class, declared=false)
    void setForUpdate(boolean forUpdate);

    @RemoteFailure(exception=RepositoryException.class, declared=false)
    boolean isForUpdate();

    @RemoteFailure(exception=RepositoryException.class, declared=false)
    IsolationLevel getIsolationLevel();
}
