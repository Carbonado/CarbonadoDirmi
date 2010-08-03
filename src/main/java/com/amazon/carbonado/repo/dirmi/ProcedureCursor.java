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

import java.util.NoSuchElementException;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.cursor.AbstractCursor;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ProcedureCursor<S> extends AbstractCursor<S> implements ProcedureOpCodes {
    private final ProcedureCall mCall;
    private final Pipe mPipe;

    private S mNext;
    private boolean mClosed;
    private Storage mCurrentStorage;

    ProcedureCursor(ProcedureCall call, Pipe pipe) {
        mCall = call;
        mPipe = pipe;
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
            try {
                mCall.close();
            } catch (RepositoryException e) {
                // Ignore.
            }
        }
    }

    private void silentClose() {
        try {
            close();
        } catch (FetchException e) {
            // Ignore.
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
            int op = mPipe.read();
            switch (op) {
            case -1: // EOF
            case OP_TERMINATOR:
                silentClose();
                return false;

            case OP_SERIALIZABLE:
                try {
                    mNext = (S) mPipe.readObject();
                } catch (ClassNotFoundException e) {
                    throw cleanup(new FetchException(e));
                } catch (ClassCastException e) {
                    // I don't actually expect a ClassCastException, because
                    // type S isn't specialized here.
                    silentClose();
                    throw e;
                }
                return true;
    
            case OP_THROWABLE:
                Throwable t = mPipe.readThrowable();
                if (t instanceof RuntimeException) {
                    silentClose();
                    throw (RuntimeException) t;
                } else {
                    FetchException fe;
                    if (t instanceof RepositoryException) {
                        fe = ((RepositoryException) t).toFetchException();
                    } else {
                        fe = new FetchException(t);
                    }
                    throw cleanup(fe);
                }

            case OP_STORABLE_NEW_TYPE:
                Class type;
                try {
                    type = (Class) mPipe.readObject();
                } catch (ClassNotFoundException e) {
                    throw cleanup(new FetchException(e));
                }

                try {
                    mCurrentStorage = mCall.mRepository.storageFor(type);
                } catch (RepositoryException e) {
                    throw cleanup(e.toFetchException());
                }

                // Fall through to next case...

            case OP_STORABLE_EXISTING_TYPE:
                try {
                    Storable s = mCurrentStorage.prepare();
                    s.readFrom(mPipe.getInputStream());
                    mNext = (S) s;
                } catch (RepositoryException e) {
                    throw cleanup(e.toFetchException());
                } catch (ClassCastException e) {
                    // I don't actually expect a ClassCastException, because
                    // type S isn't specialized here.
                    silentClose();
                    throw e;
                }
                return true;

            default:
                throw cleanup(new FetchException("Procedure call protocol error: " + op));
            }
        } catch (IOException e) {
            silentClose();
            throw new FetchException(e);
        }
    }

    public S next() throws FetchException {
        if (hasNext()) {
            S next = mNext;
            mNext = null;
            return next;
        }
        throw new NoSuchElementException();
    }

    private FetchException cleanup(FetchException cause) throws FetchException {
        silentClose();
        throw cause;
    }
}
