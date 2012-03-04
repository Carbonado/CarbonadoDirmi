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

import java.io.IOException;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.capability.RemoteProcedure;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ProcedureCall<R, D> implements RemoteProcedure.Call<R, D>, ProcedureOpCodes {
    private static final int SENDING = 0, RECEIVING = 1, CLOSED = 2;

    final ClientRepository mRepository;
    private final Pipe mPipe;
    private final boolean mInTxn;
    private int mState;
    private Class mLastStorableType;
    private StorableWriter mStorableWriter;

    ProcedureCall(ClientRepository repo, Pipe pipe, boolean inTxn) {
        mRepository = repo;
        mPipe = pipe;
        mInTxn = inTxn;
    }

    @Override
    public synchronized ProcedureCall<R, D> send(D data) throws RepositoryException {
        sendCheck();
        send0(data);
        return this;
    }

    @Override
    public synchronized ProcedureCall<R, D> sendAll(Iterable<? extends D> iterable)
        throws RepositoryException
    {
        sendCheck();
        if (iterable == null) {
            throw new IllegalArgumentException("Iterable cannot be null");
        }
        try {
            for(D data : iterable) {
                send0(data);
            }
        } catch (RepositoryException e) {
            throw cleanup(e);
        }
        return this;
    }

    @Override
    public synchronized ProcedureCall<R, D> sendAll(Cursor<? extends D> cursor)
        throws RepositoryException
    {
        sendCheck();
        if (cursor == null) {
            throw new IllegalArgumentException("Cursor cannot be null");
        }
        try {
            while (cursor.hasNext()) {
                send0(cursor.next());
            }
        } catch (RepositoryException e) {
            throw cleanup(e);
        }
        return this;
    }

    private void send0(D data) throws RepositoryException {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }

        try {
            if (!(data instanceof Storable)) {
                mPipe.writeByte(OP_SERIALIZABLE);
                mPipe.writeObject(data);
            } else {
                Storable s = (Storable) data;
                Class type = s.storableType();

                StorableWriter writer;
                if (type == mLastStorableType) {
                    writer = mStorableWriter;
                    mPipe.writeByte(OP_STORABLE_EXISTING_TYPE);
                } else {
                    try {
                        writer = ((ClientStorage) mRepository.storageFor(type)).storableWriter();
                    } catch (RepositoryException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof IOException) {
                            throw (IOException) cause;
                        }
                        throw cleanup(e);
                    }
                    mPipe.writeByte(OP_STORABLE_NEW_TYPE);
                    mPipe.writeObject(type);
                    mLastStorableType = type;
                    mStorableWriter = writer;
                }

                try {
                    writer.writeForInsert(s, mPipe.getOutputStream());
                } catch (RepositoryException e) {
                    throw cleanup(e);
                }
            }
        } catch (IOException e) {
            throw quickCleanup(e);
        }
    }

    @Override
    public synchronized ProcedureCall<R, D> reset() throws RepositoryException {
        sendCheck();
        try {
            mPipe.reset();
        } catch (IOException e) {
            throw quickCleanup(e);
        }
        return this;
    }

    @Override
    public synchronized void flush() throws RepositoryException {
        sendCheck();
        try {
            mPipe.flush();
        } catch (IOException e) {
            throw quickCleanup(e);
        }
    }

    @Override
    public synchronized Cursor<R> fetchReply() throws RepositoryException {
        try {
            executeCheck();
            return new ProcedureCursor<R>(this, mPipe);
        } catch (RepositoryException e) {
            throw cleanup(e);
        } catch (IOException e) {
            throw quickCleanup(e);
        }
    }

    @Override
    public synchronized void execute() throws RepositoryException {
        Cursor<R> c = fetchReply();
        // Fully drain in order to block until server has finished.
        try {
            while (c.skipNext(Integer.MAX_VALUE) == Integer.MAX_VALUE) {};
        } catch (FetchException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RepositoryException) {
                throw (RepositoryException) cause;
            }
            throw e;
        }
    }

    @Override
    public synchronized void executeAsync() throws RepositoryException {
        if (mInTxn) {
            throw new IllegalStateException("Current thread is in a transaction");
        }
        try {
            executeCheck();
        } catch (RepositoryException e) {
            throw cleanup(e);
        } catch (IOException e) {
            throw quickCleanup(e);
        }
        close();
    }

    @Override
    public String toString() {
        return "RemoteProcedure.Call {pipe=" + mPipe + '}';
    }

    synchronized void close() throws RepositoryException {
        if (mState != CLOSED) {
            mState = CLOSED;
            try {
                mPipe.close();
            } catch (IOException e) {
                throw new RepositoryException(e);
            }
        }
    }

    private void sendCheck() {
        if (mState != SENDING) {
            throw new IllegalStateException("Can no longer send to remote procedure");
        }
    }

    private void executeCheck() throws RepositoryException, IOException {
        if (mState != SENDING) {
            if (mState == RECEIVING) {
                throw new IllegalStateException("Already receiving reply");
            }
            throw new IllegalStateException("Already executed call");
        }

        mState = RECEIVING;

        mPipe.writeByte(OP_TERMINATOR);
        mPipe.flush();

        if (mInTxn) {
            byte op = mPipe.readByte();
            switch (op) {
            case OP_START:
                break;

            case OP_THROWABLE:
                // Transaction is invalid.
                Throwable e = mPipe.readThrowable();
                if (e instanceof RepositoryException) {
                    throw (RepositoryException) e;
                }
                throw cleanup(new RepositoryException(e));

            default:
                throw cleanup(new RepositoryException("Procedure call protocol error : " + op));
            }
        }
    }

    private synchronized RepositoryException cleanup(RepositoryException cause)
        throws RepositoryException
    {
        mState = CLOSED;
        try {
            mPipe.close();
        } catch (IOException e) {
            // Ignore.
        }
        throw cause;
    }

    private synchronized RepositoryException quickCleanup(IOException cause)
        throws RepositoryException
    {
        mState = CLOSED;
        throw new RepositoryException(cause);
    }
}
