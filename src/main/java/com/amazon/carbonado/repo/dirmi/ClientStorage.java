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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

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
    static final String TXN_INVALID_MSG =
        "Transaction is invalid, possibly due to a reconnect";

    private final Class<S> mType;
    private final ClientRepository mRepository;
    private final TriggerManager<S> mTriggerManager;
    private final InstanceFactory mInstanceFactory;
    private final ClientQueryFactory<S> mQueryFactory;

    private volatile StorageProxy<S> mStorageProxy;

    ClientStorage(Class<S> type, ClientRepository repo, RemoteStorageTransport transport)
        throws SupportException, RepositoryException
    {
        mType = type;
        mRepository = repo;
        mTriggerManager = new TriggerManager<S>(type, null);

        EnumSet<MasterFeature> features = EnumSet.noneOf(MasterFeature.class);

        try {
            // Prefer that primary key check be performed on server, to allow
            // it to fill in sequence generated values.
            features.add(MasterFeature.INSERT_NO_CHECK_PRIMARY_PK);
        } catch (NoSuchFieldError e) {
            // Using older version of Carbonado, and so primary key check will
            // be performed locally instead.
        }

        Class<? extends S> delegateStorableClass =
            DelegateStorableGenerator.getDelegateClass(type, features);

        mInstanceFactory = QuickConstructorGenerator
            .getInstance(delegateStorableClass, InstanceFactory.class);

        mQueryFactory = new ClientQueryFactory<S>(type, this);

        // Set mStorage and determine supported independent properties.
        reconnect(transport);
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
            mStorageProxy.mStorage.truncate(mRepository.localTransactionScope().getTxn());
        } catch (Exception e) {
            throw toPersistException(e);
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
        return mStorageProxy.mSupportedProperties.contains(propertyName);
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
            if (txn instanceof FailedTransaction) {
                throw new FetchException(TXN_INVALID_MSG);
            }

            StorageProxy<S> proxy = mStorageProxy;

            Pipe pipe = proxy.mStorage.tryLoad(txn, null);
            try {
                proxy.mWriter.writeForLoad(storable, pipe.getOutputStream());
                Throwable ex = pipe.readThrowable();
                if (ex != null) {
                    throw toFetchException(ex);
                }
                if (pipe.readBoolean()) {
                    storable.readFrom(pipe.getInputStream());
                    return true;
                }
                return false;
            } finally {
                pipe.close();
            }
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    public boolean doTryInsert(S storable) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            if (txn instanceof FailedTransaction) {
                throw new PersistException(TXN_INVALID_MSG);
            }

            StorageProxy<S> proxy = mStorageProxy;

            Pipe pipe = proxy.mStorage.tryInsert(txn, null);
            try {
                proxy.mWriter.writeForInsert(storable, pipe.getOutputStream());
                Throwable ex = pipe.readThrowable();
                if (ex != null) {
                    throw toPersistException(ex);
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
        } catch (Exception e) {
            throw toPersistException(e);
        }
    }

    public boolean doTryUpdate(S storable) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            if (txn instanceof FailedTransaction) {
                throw new PersistException(TXN_INVALID_MSG);
            }

            StorageProxy<S> proxy = mStorageProxy;

            Pipe pipe = proxy.mStorage.tryUpdate(txn, null);
            try {
                proxy.mWriter.writeForUpdate(storable, pipe.getOutputStream());
                Throwable ex = pipe.readThrowable();
                if (ex != null) {
                    throw toPersistException(ex);
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
        } catch (Exception e) {
            throw toPersistException(e);
        }
    }

    public boolean doTryDelete(S storable) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            if (txn instanceof FailedTransaction) {
                throw new PersistException(TXN_INVALID_MSG);
            }

            StorageProxy<S> proxy = mStorageProxy;

            Pipe pipe = proxy.mStorage.tryDelete(txn, null);
            try {
                proxy.mWriter.writeForDelete(storable, pipe.getOutputStream());
                Throwable ex = pipe.readThrowable();
                if (ex != null) {
                    throw toPersistException(ex);
                }
                return pipe.readBoolean();
            } finally {
                pipe.close();
            }
        } catch (Exception e) {
            throw toPersistException(e);
        }
    }

    long queryCount(FilterValues<S> fv, Query.Controller controller) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            RemoteStorage remote = mStorageProxy.mStorage;
            // Select remote method for compatibilty with older server.
            return controller == null
                ? remote.queryCount(fv, txn)
                : remote.queryCount(fv, txn, controller);
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    ClientCursor<S> queryFetch(FilterValues fv, OrderingList orderBy, Long from, Long to,
                               Query.Controller controller)
        throws FetchException
    {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();

            StorageProxy proxy = mStorageProxy;
            RemoteStorage remote = proxy.mStorage;

            // Select remote method for compatibilty with older server.
            Pipe pipe = controller == null
                ? remote.queryFetch(fv, orderBy, from, to, txn, null)
                : remote.queryFetch(fv, orderBy, from, to, txn, null, controller);

            ClientCursor<S> cursor = new ClientCursor<S>(this, pipe);

            if (txn != null && proxy.mProtocolVersion >= 0) {
                // Block until server has created it's cursor against the
                // transaction we just passed to it.
                if (proxy.mProtocolVersion >= 1) {
                    // Read start marker.
                    byte op = pipe.readByte();
                    if (op != RemoteStorageServer.CURSOR_START) {
                        Throwable t;
                        if (op == RemoteStorageServer.CURSOR_EXCEPTION) {
                            t = pipe.readThrowable();
                        } else {
                            t = null;
                        }
                        try {
                            cursor.close();
                        } catch (FetchException e) {
                            // Ignore.
                        }
                        if (t != null) {
                            throw t;
                        }
                        throw new FetchException("Cursor protocol error: " + op);
                    }
                } else {
                    // Fallback to potentially slower method.
                    cursor.hasNext();
                }
            }
            return cursor;
        } catch (Throwable e) {
            throw toFetchException(e);
        }
    }

    S queryLoadOne(FilterValues fv, Query.Controller controller) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            if (txn instanceof FailedTransaction) {
                throw new FetchException(TXN_INVALID_MSG);
            }

            RemoteStorage remote = mStorageProxy.mStorage;

            // Select remote method for compatibilty with older server.
            Pipe pipe = controller == null
                ? remote.queryLoadOne(fv, txn, null)
                : remote.queryLoadOne(fv, txn, null, controller);

            try {
                Throwable ex = pipe.readThrowable();
                if (ex != null) {
                    throw toFetchException(ex);
                }
                S storable = prepare();
                storable.readFrom(pipe.getInputStream());
                return storable;
            } finally {
                pipe.close();
            }
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    S queryTryLoadOne(FilterValues fv, Query.Controller controller) throws FetchException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            if (txn instanceof FailedTransaction) {
                throw new FetchException(TXN_INVALID_MSG);
            }

            RemoteStorage remote = mStorageProxy.mStorage;

            // Select remote method for compatibilty with older server.
            Pipe pipe = controller == null
                ? remote.queryTryLoadOne(fv, txn, null)
                : remote.queryTryLoadOne(fv, txn, null, controller);

            try {
                Throwable ex = pipe.readThrowable();
                if (ex != null) {
                    throw toFetchException(ex);
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
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    void queryDeleteOne(FilterValues fv, Query.Controller controller) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            RemoteStorage remote = mStorageProxy.mStorage;
            // Select remote method for compatibilty with older server.
            if (controller == null) {
                remote.queryDeleteOne(fv, txn);
            } else {
                remote.queryDeleteOne(fv, txn, controller);
            }
        } catch (Exception e) {
            throw toPersistException(e);
        }
    }

    boolean queryTryDeleteOne(FilterValues fv, Query.Controller controller)
        throws PersistException
    {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            RemoteStorage remote = mStorageProxy.mStorage;
            // Select remote method for compatibilty with older server.
            return controller == null
                ? remote.queryTryDeleteOne(fv, txn)
                : remote.queryTryDeleteOne(fv, txn, controller);
        } catch (Exception e) {
            throw toPersistException(e);
        }
    }

    void queryDeleteAll(FilterValues fv, Query.Controller controller) throws PersistException {
        try {
            RemoteTransaction txn = mRepository.localTransactionScope().getTxn();
            RemoteStorage remote = mStorageProxy.mStorage;
            // Select remote method for compatibilty with older server.
            if (controller == null) {
                remote.queryDeleteAll(fv, txn);
            } else {
                remote.queryDeleteAll(fv, txn, controller);
            }
        } catch (Exception e) {
            throw toPersistException(e);
        }
    }

    String queryPrintNative(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException
    {
        return mStorageProxy.mStorage.queryPrintNative(fv, orderBy, indentLevel);
    }

    String queryPrintPlan(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException
    {
        return mStorageProxy.mStorage.queryPrintPlan(fv, orderBy, indentLevel);
    }

    RemoteStorage remoteStorage() {
        return mStorageProxy.mStorage;
    }

    StorableWriter<S> storableWriter() {
        return mStorageProxy.mWriter;
    }

    void reconnect(RemoteStorageTransport transport) throws RepositoryException {
        RemoteStorage storage = transport.getRemoteStorage();
        StorableWriter<S> writer = ReconstructedCache.THE.writerFor(mType, transport.getLayout());

        List<String> indieList = null;
        for (StorableProperty<S> property :
                 StorableIntrospector.examine(mType).getAllProperties().values())
        {
            if (property.isIndependent()) {
                if (indieList == null) {
                    indieList = new ArrayList<String>();
                }
                indieList.add(property.getName());
            }
        }

        Set<String> supported;
        if (indieList == null) {
            supported = Collections.emptySet();
        } else {
            supported = storage.getPropertySupport(indieList.toArray(new String[0]));
        }

        mStorageProxy = new StorageProxy<S>
            (transport.getProtocolVersion(), storage, writer, supported);
    }

    /**
     * @return RepositoryException or wrapped RepositoryException
     * @throws unchecked exception
     */
    static RepositoryException toRepositoryException(Throwable e) {
        throwIfUnchecked(e);
        if (e instanceof RepositoryException) {
            return (RepositoryException) e;
        }
        return new RepositoryException(e);
    }

    /**
     * @return FetchException or wrapped FetchException
     * @throws unchecked exception
     */
    static FetchException toFetchException(Throwable e) {
        throwIfUnchecked(e);
        if (e instanceof RepositoryException) {
            return ((RepositoryException) e).toFetchException();
        }
        return new FetchException(e);
    }

    /**
     * @return PersistException or wrapped PersistException
     * @throws unchecked exception
     */
    static PersistException toPersistException(Throwable e) {
        throwIfUnchecked(e);
        if (e instanceof RepositoryException) {
            return ((RepositoryException) e).toPersistException();
        }
        return new PersistException(e);
    }

    static void throwIfUnchecked(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
    }

    public static interface InstanceFactory {
        Storable instantiate(DelegateSupport support);
    }

    // Allows several objects to be swapped-in atomically.
    private static final class StorageProxy<S extends Storable> {
        final int mProtocolVersion;
        final RemoteStorage mStorage;
        final StorableWriter<S> mWriter;
        // Cache of independent property support.
        final Set<String> mSupportedProperties;

        StorageProxy(int protocolVersion,
                     RemoteStorage storage, StorableWriter<S> writer, Set<String> supported)
        {
            mProtocolVersion = protocolVersion;
            mStorage = storage;
            mWriter = writer;
            mSupportedProperties = supported;
        }
    }
}
