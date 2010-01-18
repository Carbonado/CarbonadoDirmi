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

import java.util.EnumSet;

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.gen.DelegateStorableGenerator;
import com.amazon.carbonado.gen.DelegateSupport;
import com.amazon.carbonado.gen.MasterFeature;

import com.amazon.carbonado.qe.OrderingList;

import com.amazon.carbonado.spi.TriggerManager;

import com.amazon.carbonado.util.QuickConstructorGenerator;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ClientStorage<S extends Storable> implements Storage<S>, DelegateSupport<S> {
    private final Class<S> mType;
    private final ClientRepository mRepository;
    private final RemoteStorage mStorage;
    private final TriggerManager<S> mTriggerManager;

    private final InstanceFactory mInstanceFactory;

    private final ClientQueryFactory<S> mQueryFactory;

    ClientStorage(Class<S> type, ClientRepository repo, RemoteStorage storage)
        throws SupportException, RepositoryException
    {
        mType = type;
        mRepository = repo;
        mStorage = storage;
        mTriggerManager = new TriggerManager<S>(type, null);

        EnumSet<MasterFeature> features = EnumSet.noneOf(MasterFeature.class);

        Class<? extends S> delegateStorableClass =
            DelegateStorableGenerator.getDelegateClass(type, features);

        mInstanceFactory = QuickConstructorGenerator
            .getInstance(delegateStorableClass, InstanceFactory.class);

        mQueryFactory = new ClientQueryFactory<S>(type, this);
    }

    public Class<S> getStorableType() {
        return mType;
    }

    public S prepare() {
        return (S) mInstanceFactory.instantiate(this);
    }

    public Query<S> query() throws FetchException {
        return mQueryFactory.query();
    }

    public Query<S> query(String filter) throws FetchException {
        return mQueryFactory.query(filter);
    }

    public Query<S> query(Filter<S> filter) throws FetchException {
        return mQueryFactory.query(filter);
    }

    public void truncate() throws PersistException {
        try {
            mStorage.truncate(mRepository.localTransactionScope().getTxn());
        } catch (PersistException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.removeTrigger(trigger);
    }

    public Repository getRootRepository() {
        return mRepository;
    }

    public boolean isPropertySupported(String propertyName) {
        return prepare().isPropertySupported(propertyName);
    }

    public Trigger<? super S> getInsertTrigger() {
        return mTriggerManager.getInsertTrigger();
    }

    public Trigger<? super S> getUpdateTrigger() {
        return mTriggerManager.getUpdateTrigger();
    }

    public Trigger<? super S> getDeleteTrigger() {
        return mTriggerManager.getDeleteTrigger();
    }

    public Trigger<? super S> getLoadTrigger() {
        return mTriggerManager.getLoadTrigger();
    }

    public void locallyDisableLoadTrigger() {
        mTriggerManager.locallyDisableLoad();
    }

    public void locallyEnableLoadTrigger() {
        mTriggerManager.locallyEnableLoad();
    }

    public SequenceValueProducer getSequenceValueProducer(String name) throws PersistException {
        throw new PersistException("unsupported");
    }

    public boolean doTryLoad(S storable) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            Pipe pipe = mStorage.tryLoad(txn, null);
            try {
                // FIXME: just write the primary or alternate keys somehow
                storable.writeTo(pipe.getOutputStream());
                pipe.flush();
                RepositoryException ex = (RepositoryException) pipe.readThrowable();
                if (ex != null) {
                    throw ex.toFetchException();
                }
                if (pipe.readBoolean()) {
                    storable.readFrom(pipe.getInputStream());
                    return true;
                }
                return false;
            } finally {
                pipe.close();
            }
        } catch (FetchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FetchException(e);
        }
    }

    public boolean doTryInsert(S storable) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            Pipe pipe = mStorage.tryInsert(txn, null);
            try {
                storable.writeTo(pipe.getOutputStream());
                pipe.flush();
                RepositoryException ex = (RepositoryException) pipe.readThrowable();
                if (ex != null) {
                    throw ex.toPersistException();
                }
                int result = pipe.readByte();
                switch (result) {
                case RemoteStorageServer.STORABLE_UNCHANGED:
                    return true;
                case RemoteStorageServer.STORABLE_CHANGED:
                    storable.readFrom(pipe.getInputStream());
                    return true;
                default:
                    return false;
                }
            } finally {
                pipe.close();
            }
        } catch (PersistException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    public boolean doTryUpdate(S storable) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            Pipe pipe = mStorage.tryUpdate(txn, null);
            try {
                storable.writeTo(pipe.getOutputStream());
                pipe.flush();
                RepositoryException ex = (RepositoryException) pipe.readThrowable();
                if (ex != null) {
                    throw ex.toPersistException();
                }
                int result = pipe.readByte();
                switch (result) {
                case RemoteStorageServer.STORABLE_UNCHANGED:
                    return true;
                case RemoteStorageServer.STORABLE_CHANGED:
                    storable.readFrom(pipe.getInputStream());
                    return true;
                default:
                    return false;
                }
            } finally {
                pipe.close();
            }
        } catch (PersistException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    public boolean doTryDelete(S storable) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            Pipe pipe = mStorage.tryDelete(txn, null);
            try {
                // FIXME: just write the primary or alternate keys somehow
                storable.writeTo(pipe.getOutputStream());
                pipe.flush();
                RepositoryException ex = (RepositoryException) pipe.readThrowable();
                if (ex != null) {
                    throw ex.toPersistException();
                }
                if (pipe.readBoolean()) {
                    storable.readFrom(pipe.getInputStream());
                    return true;
                }
                return false;
            } finally {
                pipe.close();
            }
        } catch (PersistException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    long queryCount(FilterValues<S> fv) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            return mStorage.queryCount(fv, txn);
        } catch (FetchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FetchException(e);
        }
    }

    Pipe queryFetch(FilterValues fv, OrderingList orderBy) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            return mStorage.queryFetch(fv, orderBy, null, null, txn, null);
        } catch (FetchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FetchException(e);
        }
    }

    Pipe queryFetch(FilterValues fv, OrderingList orderBy, long from, Long to)
        throws FetchException
    {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            return mStorage.queryFetch(fv, orderBy, from, to, txn, null);
        } catch (FetchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FetchException(e);
        }
    }

    S queryLoadOne(FilterValues fv) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            Pipe pipe = mStorage.queryLoadOne(fv, txn, null);
            try {
                RepositoryException ex = (RepositoryException) pipe.readThrowable();
                if (ex != null) {
                    throw ex.toFetchException();
                }
                S storable = prepare();
                storable.readFrom(pipe.getInputStream());
                return storable;
            } finally {
                pipe.close();
            }
        } catch (FetchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FetchException(e);
        }
    }

    S queryTryLoadOne(FilterValues fv) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            Pipe pipe = mStorage.queryTryLoadOne(fv, txn, null);
            try {
                RepositoryException ex = (RepositoryException) pipe.readThrowable();
                if (ex != null) {
                    throw ex.toFetchException();
                }
                if (pipe.readBoolean()) {
                    S storable = prepare();
                    storable.readFrom(pipe.getInputStream());
                    return storable;
                }
                return null;
            } finally {
                pipe.close();
            }
        } catch (FetchException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FetchException(e);
        }
    }

    void queryDeleteOne(FilterValues fv) throws PersistException {
        try {
            mStorage.queryDeleteOne(fv, mRepository.localTransactionScope().getTxn());
        } catch (FetchException e) {
            throw e.toPersistException();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    boolean queryTryDeleteOne(FilterValues fv) throws PersistException {
        try {
            return mStorage.queryTryDeleteOne(fv, mRepository.localTransactionScope().getTxn());
        } catch (FetchException e) {
            throw e.toPersistException();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    void queryDeleteAll(FilterValues fv) throws PersistException {
        try {
            mStorage.queryDeleteAll(fv, mRepository.localTransactionScope().getTxn());
        } catch (FetchException e) {
            throw e.toPersistException();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistException(e);
        }
    }

    String queryPrintNative(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException
    {
        return mStorage.queryPrintNative(fv, orderBy, indentLevel);
    }

    String queryPrintPlan(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException
    {
        return mStorage.queryPrintPlan(fv, orderBy, indentLevel);
    }

    public static interface InstanceFactory {
        Storable instantiate(DelegateSupport support);
    }
}
