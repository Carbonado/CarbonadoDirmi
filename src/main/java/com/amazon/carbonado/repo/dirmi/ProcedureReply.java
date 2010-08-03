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

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.capability.RemoteProcedure;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ProcedureReply<R> implements RemoteProcedure.Reply<R>, ProcedureOpCodes {
    private static final int REPLYING = 0, CLOSED = 1;

    private final RemoteProcedureExecutorServer mProcedureExecutor;
    private final ProcedureRequest mRequest;
    private final Pipe mPipe;

    private int mState;
    private Class mLastStorableType;
    private StorableWriter mStorableWriter;

    ProcedureReply(RemoteProcedureExecutorServer executor, ProcedureRequest request, Pipe pipe) {
        mProcedureExecutor = executor;
        mRequest = request;
        mPipe = pipe;
    }

    @Override
    public synchronized ProcedureReply<R> send(R data) throws RepositoryException {
        sendCheck();
        send0(data);
        return this;
    }

    @Override
    public synchronized ProcedureReply<R> sendAll(Iterable<? extends R> iterable)
        throws RepositoryException
    {
        sendCheck();
        if (iterable == null) {
            throw new IllegalArgumentException("Iterable cannot be null");
        }
        try {
            for(R data : iterable) {
                send0(data);
            }
        } catch (RepositoryException e) {
            throw cleanup(e);
        }
        return this;
    }

    @Override
    public synchronized ProcedureReply<R> sendAll(Cursor<? extends R> cursor)
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

    private void send0(R data) throws RepositoryException {
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
                        writer = mProcedureExecutor.writerFor(type);
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
                    writer.writeLoadResponse(s, mPipe.getOutputStream());
                } catch (RepositoryException e) {
                    throw cleanup(e);
                }
            }
        } catch (IOException e) {
            throw quickCleanup(e);
        }
    }

    @Override
    public synchronized ProcedureReply<R> reset() throws RepositoryException {
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
    public void finish() throws RepositoryException {
        if (mState != CLOSED) {
            mState = CLOSED;
            mRequest.close();
        }
    }

    @Override
    public String toString() {
        return "RemoteProcedure.Reply {pipe=" + mPipe + '}';
    }

    private void sendCheck() {
        if (mState != REPLYING) {
            throw new IllegalStateException("Can no longer send to caller");
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
