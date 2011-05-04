/*
 * Copyright 2011 Amazon Technologies, Inc. or its affiliates.
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

import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.Query;

/**
 * Used to stop queries when remote Session is closed.
 *
 * @author Brian S O'Neill
 */
class UnreferencedController implements Query.Controller {
    // Set to true by RemoteStorageServer.
    volatile boolean mUnreferenced;

    /**
     * Returns a Controller which combines this and the one given. Returns this
     * if given controller is null.
     */
    Query.Controller merge(Query.Controller other) {
        return other == null ? this : new Merged(other);
    }

    @Override
    public void continueCheck() throws FetchInterruptedException {
        if (mUnreferenced) {
            throw new FetchInterruptedException("Remote session is closed");
        }
    }

    @Override
    public long getTimeout() {
        // No timeout -- only check if unreferenced.
        return -1;
    }

    @Override
    public TimeUnit getTimeoutUnit() {
        // No timeout -- only check if unreferenced.
        return null;
    }

    @Override
    public void begin() {
        // Nothing to do.
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    private class Merged implements Query.Controller {
        private final Query.Controller mOther;

        Merged(Query.Controller other) {
            mOther = other;
        }

        @Override
        public void continueCheck() throws FetchException {
            // Check remote session first, since its exception message is
            // expected to be more informative.
            UnreferencedController.this.continueCheck();
            mOther.continueCheck();
        }

        @Override
        public long getTimeout() {
            return mOther.getTimeout();
        }

        @Override
        public TimeUnit getTimeoutUnit() {
            return mOther.getTimeoutUnit();
        }

        @Override
        public void begin() {
            mOther.begin();
        }

        @Override
        public void close() {
            mOther.close();
        }
    }
}
