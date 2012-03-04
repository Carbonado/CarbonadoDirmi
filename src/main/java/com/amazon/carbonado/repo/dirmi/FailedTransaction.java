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

import java.io.Serializable;

import org.cojen.dirmi.Asynchronous;
import org.cojen.dirmi.Batched;
import org.cojen.dirmi.RemoteFailure;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;

/**
 *  Remote transaction whose presence indicates a failed transaction.
 *
 * @author Olga Kuznetsova
 */
class FailedTransaction implements Serializable, RemoteTransaction {
    public void commit() {
	throw new UnsupportedOperationException();
    }

    public void exit() {
       	throw new UnsupportedOperationException();
    }

    public void setForUpdate(boolean forUpdate) {
	throw new UnsupportedOperationException();
    }

    public boolean isForUpdate() {
	throw new UnsupportedOperationException();
    }

    public IsolationLevel getIsolationLevel() {
	throw new UnsupportedOperationException();
    }
}
