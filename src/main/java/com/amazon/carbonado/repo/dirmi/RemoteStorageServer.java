/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

    private final Storage mStorage;

    RemoteStorageServer(Storage storage) throws SupportException {
        mStorage = storage;
    }

    public Pipe tryLoad(RemoteTransaction txn, Pipe pipe) {
        attach(txn);
        try {
            Storable s = mStorage.prepare();

            boolean loaded;
            try {
                s.readFrom(pipe.getInputStream());
                loaded = s.tryLoad();
            } catch (RepositoryException e) {
                pipe.writeThrowable(e);
                return null;
            }

            pipe.writeThrowable(null);

            if (loaded) {
                pipe.writeBoolean(true);
                s.writeTo(pipe.getOutputStream());
            } else {
                pipe.writeBoolean(false);
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
        attach(txn);
        try {
            Storable s = mStorage.prepare();

            boolean inserted;
            try {
                s.readFrom(pipe.getInputStream());
                inserted = s.tryInsert();
            } catch (RepositoryException e) {
                pipe.writeThrowable(e);
                return null;
            }

            pipe.writeThrowable(null);

            if (inserted) {
                // FIXME: As an optimization, pass nothing back if unchanged
                pipe.write(STORABLE_CHANGED);
                s.writeTo(pipe.getOutputStream());
            } else {
                pipe.write(STORABLE_CHANGE_FAILED);
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
        attach(txn);
        try {
            Storable s = mStorage.prepare();

            boolean updated;
            try {
                s.readFrom(pipe.getInputStream());
                updated = s.tryUpdate();
            } catch (RepositoryException e) {
                pipe.writeThrowable(e);
                return null;
            }

            pipe.writeThrowable(null);

            if (updated) {
                // FIXME: As an optimization, pass nothing back if unchanged
                pipe.write(STORABLE_CHANGED);
                s.writeTo(pipe.getOutputStream());
            } else {
                pipe.write(STORABLE_CHANGE_FAILED);
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
        attach(txn);
        try {
            Storable s = mStorage.prepare();

            boolean deleted;
            try {
                s.readFrom(pipe.getInputStream());
                deleted = s.tryDelete();
            } catch (RepositoryException e) {
                pipe.writeThrowable(e);
                return null;
            }

            pipe.writeThrowable(null);
            pipe.writeBoolean(deleted);
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
        attach(txn);
        try {
            return buildQuery(fv, null).count();
        } finally {
            detach(txn);
        }
    }

    public Pipe queryFetch(FilterValues fv, OrderingList orderBy, Long from, Long to,
                           RemoteTransaction txn, Pipe pipe)
        throws FetchException
    {
        attach(txn);
        try {
            OutputStream out = pipe.getOutputStream();
            try {
                Query query = buildQuery(fv, orderBy);
                Cursor cursor;
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
                    while (cursor.hasNext()) {
                        Storable s = (Storable) cursor.next();
                        out.write(CURSOR_STORABLE);
                        s.writeTo(out);
                    }
                } finally {
                    cursor.close();
                }
                out.write(CURSOR_END);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                out.write(CURSOR_EXCEPTION);
                pipe.writeObject(e);
            }
        } catch (IOException e) {
            throw new FetchException(e);
        } finally {
            detach(txn);
            try {
                pipe.close();
            } catch (IOException e) {
                // Don't care.
            }
        }
        return null;
    }

    public Pipe queryLoadOne(FilterValues fv, RemoteTransaction txn, Pipe pipe) {
        attach(txn);
        try {
            Storable s;
            try {
                s = buildQuery(fv, null).loadOne();
            } catch (RepositoryException e) {
                pipe.writeThrowable(e);
                return null;
            }

            pipe.writeThrowable(null);
            s.writeTo(pipe.getOutputStream());
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
        attach(txn);
        try {
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
                s.writeTo(pipe.getOutputStream());
            } else {
                pipe.writeBoolean(false);
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
        attach(txn);
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
        attach(txn);
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
        attach(txn);
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
        attach(txn);
        try {
            mStorage.truncate();
        } finally {
            detach(txn);
        }
    }

    private void attach(RemoteTransaction txn) {
        if (txn != null) {
            ((RemoteTransactionServer) txn).attach();
        }
    }

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
}