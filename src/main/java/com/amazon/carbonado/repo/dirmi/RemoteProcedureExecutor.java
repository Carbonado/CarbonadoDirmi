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

import java.rmi.Remote;

import org.cojen.dirmi.Asynchronous;
import org.cojen.dirmi.CallMode;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteFailure;

import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.capability.RemoteProcedure;

/**
 * Remote interface for executing remote procedure calls. Defining this as a
 * separate interface (instead of folding into RemoteRepository) allows the
 * server implementation to cache session-specific serialization logic.
 *
 * @author Brian S O'Neill
 */
public interface RemoteProcedureExecutor extends Remote {
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=RepositoryException.class)
    Pipe remoteCall(RemoteTransaction txn, RemoteProcedure proc, Pipe pipe)
        throws RepositoryException;
}
