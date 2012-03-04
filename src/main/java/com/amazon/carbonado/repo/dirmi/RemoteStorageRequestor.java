/*
 * Copyright 2010-2012 Amazon Technologies, Inc. or its affiliates.
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
import java.rmi.RemoteException;

import org.cojen.dirmi.RemoteFailure;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

/**
 * Callback used by {@link RemoteProcedureExecutor} for serializing Storables
 * to clients.
 *
 * @author Brian S O'Neill
 */
public interface RemoteStorageRequestor extends Remote {
    @RemoteFailure(exception=RepositoryException.class)
    RemoteStorage serverStorageFor(Class<? extends Storable> type) throws RepositoryException;
}
