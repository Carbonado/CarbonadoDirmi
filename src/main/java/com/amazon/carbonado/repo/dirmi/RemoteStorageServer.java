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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.HashSet;
import java.util.Set;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.qe.OrderingList;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RemoteStorageServer implements RemoteStorage {
    static final byte STORABLE_CHANGED = 0;
    static final byte STORABLE_UNCHANGED = 1;
    static final byte STORABLE_CHANGE_FAILED = 2;

    static final byte CURSOR_STORABLE = 0;
    static final byte CURSOR_EXCEPTION = 1;
    static final byte CURSOR_END = 2;
    static final byte CURSOR_START = 3;

    private static final int FETCH_BATCH_SIZE = 100;

    private final Storage mStorage;
    private final StorableWriter mWriter;
    private final boolean mWriteStartMarker;

    RemoteStorageServer(Storage storage, StorableWriter writer, boolean writeStartMarker)
        throws SupportException
    {
        mStorage = storage;
        mWriter = writer;
        mWriteStartMarker = writeStartMarker;
    }

    public Pipe tryLoad(RemoteTransaction txn, Pipe pipe) {
        try {
            Storable s = mStorage.prepare();
            try {
                s.readFrom(pipe.getInputStream());
            } catch (SupportException e) {
                pipe.writeThrowable(e);
                return null;
            }

            if (attachFetch(txn, pipe)) {
                boolean loaded;
                try {
                    loaded = s.tryLoad();
                } catch (RepositoryException e) {
                    pipe.writeThrowable(e);
                    return null;
                }
                
                pipe.writeThrowable(null);
                
                if (loaded) {
                    pipe.writeBoolean(true);
                    mWriter.writeLoadResponse(s, pipe.getOutputStream());
                } else {
                    pipe.writeBoolean(false);
                }
            } else {
                txn = null;
            }
        } catch (IOException e) {
            // Ignore.
        } catch (SupportException e) {
            // Ignore. 
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }

        return null;
    }

    public Pipe tryInsert(RemoteTransaction txn, Pipe pipe) {
        try {
            Storable s = mStorage.prepare();
            try {
                s.readFrom(pipe.getInputStream());
            } catch (SupportException e) {
                pipe.writeThrowable(e);
                return null;
            }

            if (attachPersist(txn, pipe)) {
                boolean inserted;
                try {
                    inserted = s.tryInsert();
                } catch (RepositoryException e) {
                    pipe.writeThrowable(e);
                    return null;
                }
                
                pipe.writeThrowable(null);
                
                if (inserted) {
                    // TODO: As an optimization, pass nothing back if unchanged
                    pipe.write(STORABLE_CHANGED);
                    mWriter.writeInsertResponse(s, pipe.getOutputStream());
                } else {
                    pipe.write(STORABLE_CHANGE_FAILED);
                }
            } else {
                txn = null;
            }
        } catch (IOException e) {
            // Ignore.
        } catch (SupportException e) {
            // Ignore.
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }

        return null;
    }

    public Pipe tryUpdate(RemoteTransaction txn, Pipe pipe) {
        try {
            Storable s = mStorage.prepare();
            try {
                s.readFrom(pipe.getInputStream());
            } catch (SupportException e) {
                pipe.writeThrowable(e);
                return null;
            }

            if (attachPersist(txn, pipe)) {
                boolean updated;
                try {
                    updated = s.tryUpdate();
                } catch (RepositoryException e) {
                    pipe.writeThrowable(e);
                    return null;
                }
                
                pipe.writeThrowable(null);
                
                if (updated) {
                    // TODO: As an optimization, pass nothing back if unchanged
                    pipe.write(STORABLE_CHANGED);
                    mWriter.writeUpdateResponse(s, pipe.getOutputStream());
                } else {
                    pipe.write(STORABLE_CHANGE_FAILED);
                }
            } else {
                txn = null;
            }
        } catch (IOException e) {
            // Ignore.
        } catch (SupportException e) {
            // Ignore.
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }

        return null;
    }

    public Pipe tryDelete(RemoteTransaction txn, Pipe pipe) {
        try {
            Storable s = mStorage.prepare();
            try {
                s.readFrom(pipe.getInputStream());
            } catch (SupportException e) {
                pipe.writeThrowable(e);
                return null;
            }

            if (attachPersist(txn, pipe)) {
                boolean deleted;
                try {
                    deleted = s.tryDelete();
                } catch (RepositoryException e) {
                    pipe.writeThrowable(e);
                    return null;
                }
                
                pipe.writeThrowable(null);
                pipe.writeBoolean(deleted);
            } else {
                txn = null;
            } 
        } catch (IOException e) {
            // Ignore.
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }

        return null;
    }

    public long queryCount(FilterValues fv, RemoteTransaction txn) throws FetchException {
        attachFetch(txn);
        try {
            return buildQuery(fv, null).count();
        } finally {
            detach(txn);
        }
    }

    public Pipe queryFetch(FilterValues fv, OrderingList orderBy, Long from, Long to,
                           RemoteTransaction txn, Pipe pipe)
    {
        try {
            OutputStream out = pipe.getOutputStream();
            try {
                Query query = buildQuery(fv, orderBy);
                Cursor cursor;

                attachFetch(txn);
                try {
                    if (from == null) {
                        if (to == null) {
                            cursor = query.fetch();
                        } else {
                            cursor = query.fetchSlice(0, to);
                        }
                    } else {
                        cursor = query.fetchSlice(from, to);
                    }

                    try {
                        if (txn != null && mWriteStartMarker) {
                            out.write(CURSOR_START);
                        }

                        // Another thread might want access to the transaction
                        // while fetching from the cursor. Detach cursor from
                        // thread while writing over pipe, which is a blocking
                        // operation. To reduce overhead of attach/detach,
                        // operate over batches.

                        final Storable[] batch = new Storable[FETCH_BATCH_SIZE];
                        final RemoteTransaction originalTxn = txn;

                        while (true) {
                            int size = 0;
                            while (cursor.hasNext()) {
                                batch[size++] = (Storable) cursor.next();
                                if (size >= batch.length) {
                                    break;
                                }
                            }

                            if (size == 0) {
                                break;
                            }

                            // Detach while writing batch, and temporarily set
                            // txn to null to prevent detach in outer finally
                            // block from functioning.
                            detach(txn);
                            txn = null;

                            for (int i=0; i<size; i++) {
                                out.write(CURSOR_STORABLE);
                                mWriter.writeLoadResponse(batch[i], out);
                                batch[i] = null;
                            }

                            if (size < batch.length) {
                                // Incomplete batch because cursor has finished.
                                break;
                            }

                            // Re-attach and fetch another batch.
                            attachFetch(originalTxn);
                            txn = originalTxn;
                        }
                    } finally {
                        cursor.close();
                    }
                } finally {
                    detach(txn);
                }

                out.write(CURSOR_END);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                out.write(CURSOR_EXCEPTION);
                pipe.writeThrowable(e);
            }
        } catch (IOException e) {
            // Ignore.
        } finally {
            try {
                pipe.close();
            } catch (IOException e) {
                // Don't care.
            }
        }
        return null;
    }

    public Pipe queryLoadOne(FilterValues fv, RemoteTransaction txn, Pipe pipe) {
        try {
            if (attachFetch(txn, pipe)) {
                Storable s;
                try {
                    s = buildQuery(fv, null).loadOne();
                } catch (RepositoryException e) {
                    pipe.writeThrowable(e);
                    return null;
                }
                
                pipe.writeThrowable(null);
                mWriter.writeLoadResponse(s, pipe.getOutputStream());
            } else {
                txn = null;
            }
        } catch (IOException e) {
            // Ignore.
        } catch (SupportException e) {
            // Ignore.
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }

        return null;
    }

    public Pipe queryTryLoadOne(FilterValues fv, RemoteTransaction txn, Pipe pipe) {
        try {
            if (attachFetch(txn, pipe)) {
                Storable s;
                try {
                    s = buildQuery(fv, null).tryLoadOne();
                } catch (RepositoryException e) {
                    pipe.writeThrowable(e);
                    return null;
                }
                
                pipe.writeThrowable(null);
                
                if (s != null) {
                    pipe.writeBoolean(true);
                    mWriter.writeLoadResponse(s, pipe.getOutputStream());
                } else {
                    pipe.writeBoolean(false);
                }
            } else {
                txn = null;
            }
        } catch (IOException e) {
            // Ignore.
        } catch (SupportException e) {
            // Ignore.
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Ignore.
            }
        }

        return null;
    }

    public void queryDeleteOne(FilterValues fv, RemoteTransaction txn)
        throws FetchException, PersistException
    {
        attachPersist(txn);
        try {
            Query query = buildQuery(fv, null);
            query.deleteOne();
        } finally {
            detach(txn);
        }
    }

    public boolean queryTryDeleteOne(FilterValues fv, RemoteTransaction txn)
        throws FetchException, PersistException
    {
        attachPersist(txn);
        try {
            Query query = buildQuery(fv, null);
            return query.tryDeleteOne();
        } finally {
            detach(txn);
        }
    }

    public void queryDeleteAll(FilterValues fv, RemoteTransaction txn)
        throws FetchException, PersistException
    {
        attachPersist(txn);
        try {
            buildQuery(fv, null).deleteAll();
        } finally {
            detach(txn);
        }
    }

    public String queryPrintNative(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException
    {
        Query query = buildQuery(fv, orderBy);
        StringBuilder builder = new StringBuilder(); 
        try {
            if (!query.printNative(builder, indentLevel)) {
                return null;
            }
        } catch (IOException e) {
            // Not gonna happen.
        }
        return builder.toString();
    }

    public String queryPrintPlan(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException
    {
        Query query = buildQuery(fv, orderBy);
        StringBuilder builder = new StringBuilder(); 
        try {
            if (!query.printPlan(builder, indentLevel)) {
                return null;
            }
        } catch (IOException e) {
            // Not gonna happen.
        }
        return builder.toString();
    }

    public void truncate(RemoteTransaction txn) throws PersistException {
        attachPersist(txn);
        try {
            mStorage.truncate();
        } finally {
            detach(txn);
        }
    }

    public Set<String> getPropertySupport(String... propertyNames) {
        Storable s = mStorage.prepare();
        Set<String> supported = null;
        for (int i=0; i<propertyNames.length; i++) {
            if (s.isPropertySupported(propertyNames[i])) {
                if (supported == null) {
                    supported = new HashSet<String>();
                }
                supported.add(propertyNames[i]);
            }
        }
        return supported;
    }

    private void attachFetch(RemoteTransaction txn) throws FetchException {
        if (txn != null) {
            try {
                ((RemoteTransactionServer) txn).attach();
            } catch (ClassCastException e) {
                throw new FetchException("Transaction is invalid due to reconnect");
            }
        }
    }

    /**
     * Throwing a ClassCastException means that a reconnect happened, so when this
     * transaction from the old repository tries to attach, it is attaching to a 
     * different repository than the one that was there previously. The exception
     * is put into the pipe to let the user know that the transaction will not be 
     * able to attach and then commit any changes that were made during it. 
     *
     * @returns true if attach succeeded, false if exception was written to pipe.
     */
    private boolean attachFetch(RemoteTransaction txn, Pipe pipe) {
        try {
            attachFetch(txn);
        } catch (FetchException e) {
            try {
                pipe.writeThrowable(e);
            } catch (IOException e2) {
                // Ignore.
            }
            return false;
        }
        return true;
    }

    private void attachPersist(RemoteTransaction txn) throws PersistException {
        if (txn != null) {
            try {
                ((RemoteTransactionServer) txn).attach();
            } catch (ClassCastException e) {
                throw new PersistException("Transaction is invalid due to reconnect");
            }
        }
    }

    /**
     * Throwing a ClassCastException means that a reconnect happened, so when this
     * transaction from the old repository tries to attach, it is attaching to a 
     * different repository than the one that was there previously. The exception
     * is put into the pipe to let the user know that the transaction will not be 
     * able to attach and then commit any changes that were made during it. 
     * 
     * @returns true if attach succeeded, false if exception was written to pipe.
     */
    private boolean attachPersist(RemoteTransaction txn, Pipe pipe) {
        try {
            attachPersist(txn);
        } catch (PersistException e) {
            try {
                pipe.writeThrowable(e);
            } catch (IOException e2) {
                // Ignore.
            }
            return false;
        }
        return true;
    }

    /**
     * Detach does not need to check for a ClassCastException because the same
     * transaction is being operated upon. The check that the transaction is
     * still of the form of RemoteTransactionServer was already taken place in
     * the attach method so since the same transaction is operated upon, the
     * exception cannot be thrown.
     */
    private void detach(RemoteTransaction txn) {
        if (txn != null) {
            ((RemoteTransactionServer) txn).detach();
        }
    }

    private Query buildQuery(FilterValues fv, OrderingList orderBy) throws FetchException {
        Query query;
        if (fv == null) {
            query = mStorage.query();
        } else {
            query = mStorage.query(fv.getFilter()).withValues(fv.getSuppliedValues());
        }
        if (orderBy != null && orderBy.size() > 0) {
            int length = orderBy.size();
            String[] orderByNames = new String[length];
            for (int i=0; i<length; i++) {
                orderByNames[i] = orderBy.get(i).toString();
            }
            query = query.orderBy(orderByNames);
        }
        return query;
    }

    StorableWriter storableWriter() {
        return mWriter;
    }
}
