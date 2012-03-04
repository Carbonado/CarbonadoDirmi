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

import java.util.Collection;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.capability.RemoteProcedure;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ProcedureRequest<R, D> implements RemoteProcedure.Request<R, D>, ProcedureOpCodes {
    private static final int RECEIVING = 0, READY_TO_SEND = 1, SENDING = 2, CLOSED = 3;

    private final RemoteProcedureExecutorServer mProcedureExecutor;
    private final Pipe mPipe;
    private final RemoteTransaction mTxn;
    private int mState;
    private Storage mCurrentStorage;

    ProcedureRequest(RemoteProcedureExecutorServer executor, Pipe pipe, RemoteTransaction txn) {
        mProcedureExecutor = executor;
        mPipe = pipe;
        mTxn = txn;
    }

    @Override
    public synchronized D receive() throws RepositoryException {
        if (mState != RECEIVING) {
            return null;
        }
        return receive0();
    }

    @Override
    public synchronized int receiveInto(Collection<? super D> c) throws RepositoryException {
        if (c == null) {
            throw new IllegalArgumentException("Collection cannot be null");
        }
        int amount = 0;
        if (mState == RECEIVING) {
            D data;
            while ((data = receive0()) != null) {
                c.add(data);
                amount++;
            }
        }
        return amount;
    }

    private D receive0() throws RepositoryException {
        try {
            byte op = mPipe.readByte();
            switch (op) {
            case OP_TERMINATOR:
                mState = READY_TO_SEND;
                return null;

            case OP_SERIALIZABLE:
                try {
                    return (D) mPipe.readObject();
                } catch (Throwable e) {
                    throw cleanup(e);
                }

            case OP_THROWABLE:
                throw cleanup(mPipe.readThrowable());

            case OP_STORABLE_NEW_TYPE:
                try {
                    Class type = (Class) mPipe.readObject();
                    mCurrentStorage =
                        mProcedureExecutor.mRepositoryServer.mRepository.storageFor(type);
                } catch (Throwable e) {
                    throw cleanup(e);
                }

                // Fall through to next case...

            case OP_STORABLE_EXISTING_TYPE:
                try {
                    Storable s = mCurrentStorage.prepare();
                    s.readFrom(mPipe.getInputStream());
                    return (D) s;
                } catch (Throwable e) {
                    throw cleanup(e);
                }

            default:
                throw cleanup(new RepositoryException("Procedure call protocol error: " + op));
            }
        } catch (IOException e) {
            throw quickCleanup(e);
        }
    }

    @Override
    public synchronized RemoteProcedure.Reply<R> beginReply()
        throws IllegalStateException, RepositoryException
    {
        replyCheck(false, false, false);
        return new ProcedureReply<R>(mProcedureExecutor, this, mPipe);
    }

    @Override
    public synchronized void finish() throws IllegalStateException, RepositoryException {
        replyCheck(true, false, false);
        close();
    }

    private void replyCheck(boolean forFinish, boolean forSilent, boolean pendingException)
        throws IllegalStateException, RepositoryException
    {
        if (mState != READY_TO_SEND) {
            if (mState == CLOSED) {
                if (forFinish) {
                    return;
                }
                throw new IllegalStateException("Request is finished");
            }
            if (mState == RECEIVING) {
                // Try to read terminator.
                receive0();
                if (mState == RECEIVING) {
                    // Send exception to client, but all remaining data must be drained.
                    while (receive0() != null) {}

                    if (!pendingException) {
                        IllegalStateException ex = new IllegalStateException
                            ("Procedure cannot reply or finish until all data has been received");
                        try {
                            mPipe.writeByte(OP_THROWABLE);
                            mPipe.writeThrowable(ex);
                        } catch (IOException e) {
                            // Ignore.
                        } finally {
                            silentClose();
                        }

                        // Also throw to server, so it knows that something is broken.
                        throw ex;
                    }
                }
            }
            if (mState == SENDING) {
                if (forSilent) {
                    return;
                }
                throw new IllegalStateException("Reply already begun");
            }
        }

        mState = SENDING;

        if (mTxn != null) {
            // If this point is reached, then any transaction was successfully
            // attached and no exception was thrown.
            try {
                mPipe.writeByte(OP_START);
                // Flush to unblock caller which is waiting for attach
                // confirmation.
                mPipe.flush();
            } catch (IOException e) {
                throw quickCleanup(e);
            }
        }
    }

    @Override
    public String toString() {
        return "RemoteProcedure.Request {pipe=" + mPipe + '}';
    }

    synchronized void silentFinish() throws IllegalStateException {
        try {
            replyCheck(true, true, false);
            close();
        } catch (RepositoryException e) {
            // Ignore.
        }
    }

    /**
     * @throws original cause if it cannot be written back
     */
    synchronized void silentFinish(Throwable cause) throws IllegalStateException, Throwable {
        try {
            replyCheck(true, true, true);
        } catch (RepositoryException e) {
            // Ignore.
        }

        try {
            mPipe.writeByte(ProcedureOpCodes.OP_THROWABLE);
            mPipe.writeThrowable(cause);
        } catch (IOException e2) {
            // Pipe might still be in a reading state.
            throw cause;
        } finally {
            try {
                close();
            } catch (RepositoryException e) {
                // Ignore.
            }
        }
    }

    synchronized void close() throws RepositoryException {
        if (mState != CLOSED) {
            mState = CLOSED;
            if (mTxn != null) {
                // Detach before writing terminator to avoid race condition
                // with client. It might otherwise proceed and then try to
                // remotely attach the transaction to a different thread.
                ((RemoteTransactionServer) mTxn).detach();
            }
            try {
                try {
                    mPipe.writeByte(OP_TERMINATOR);
                } catch (IOException e) {
                    // Pipe might still be in a reading state.
                }
                mPipe.close();
            } catch (IOException e) {
                throw new RepositoryException(e);
            }
        }
    }

    private synchronized void silentClose() {
        if (mState != CLOSED) {
            mState = CLOSED;
            if (mTxn != null) {
                ((RemoteTransactionServer) mTxn).detach();
            }
            try {
                mPipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }
    }

    private synchronized RepositoryException cleanup(Throwable cause)
        throws RepositoryException
    {
        silentClose();
        throw ClientStorage.toRepositoryException(cause);
    }

    private synchronized RepositoryException quickCleanup(IOException cause)
        throws RepositoryException
    {
        if (mState != CLOSED) {
            mState = CLOSED;
            if (mTxn != null) {
                ((RemoteTransactionServer) mTxn).detach();
            }
        }
        throw new RepositoryException(cause);
    }
}
