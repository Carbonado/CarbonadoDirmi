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

import java.io.InputStream;
import java.io.IOException;

import java.util.NoSuchElementException;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.AbstractCursor;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ClientCursor<S extends Storable> extends AbstractCursor<S> {
    private final ClientStorage<S> mStorage;
    private final Pipe mPipe;

    private S mNext;
    private boolean mClosed;

    ClientCursor(ClientStorage<S> storage, Pipe pipe) throws FetchException {
	mStorage = storage;
        mPipe = pipe;
	
	// Called to ensure that server has detached from thread local 
	// transaction at this point
	this.hasNext(); 
    }

    public void close() throws FetchException {
        if (!mClosed) {
            mNext = null;
            mClosed = true;
            try {
                mPipe.close();
            } catch (IOException e) {
                throw new FetchException(e);
            }
        }
    }

    public boolean hasNext() throws FetchException {
        if (mNext != null) {
            return true;
        }

        if (mClosed) {
            return false;
        }

        try {
            byte type = mPipe.readByte();
            if (type == RemoteStorageServer.CURSOR_STORABLE) {
                S next = mStorage.prepare();
                next.readFrom(mPipe.getInputStream());
                mNext = next;
                return true;
            } else if (type == RemoteStorageServer.CURSOR_EXCEPTION) {
                throw (Exception) mPipe.readObject();
            }
            mClosed = true;
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            if (e instanceof FetchException) {
                throw (FetchException) e;
            }
            throw new FetchException(e);
        }

        return false;
    }

    public S next() throws FetchException {
        if (hasNext()) {
            S next = mNext;
            mNext = null;
            return next;
        }
        throw new NoSuchElementException();
    }
}
