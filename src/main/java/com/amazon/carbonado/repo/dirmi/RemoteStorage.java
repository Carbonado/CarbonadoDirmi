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

import java.rmi.Remote;

import java.util.Set;

import org.cojen.dirmi.Asynchronous;
import org.cojen.dirmi.CallMode;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteFailure;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.qe.OrderingList;

/**
 * Remote storage definition which does not actually extend the Storage
 * interface. Client class is responsible for adapting Storage calls to
 * RemoteStorage calls.
 *
 * <p>Note: All access to Storable and Query objects is inlined. There is no
 * reason to make them remote objects as that would add unnecessary
 * latency. Most operations on Storables and Queries can be performed locally.
 *
 * @author Brian S O'Neill
 */
public interface RemoteStorage extends Remote {
    /**
     * Returns serialized storable properties, using the serialized
     * key. Returns null if not found.
     *
     * @param txn optional
     * @param pipe send serialized key properties
     * @return true if loaded
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=FetchException.class)
    Pipe tryLoad(RemoteTransaction txn, Pipe pipe) throws FetchException;

    /**
     * Inserts the given serialized storable, returing the deserialized
     * properties. Returns false if storable did not change after insert.
     *
     * @param txn optional
     * @param pipe send fully serialized storable
     * @return true if inserted
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=PersistException.class)
    Pipe tryInsert(RemoteTransaction txn, Pipe pipe) throws PersistException;

    /**
     * Updates the given serialized storable, returing the deserialized
     * properties.
     *
     * @param txn optional
     * @param pipe send fully serialized storable
     * @return true if updated
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=PersistException.class)
    Pipe tryUpdate(RemoteTransaction txn, Pipe pipe) throws PersistException;

    /**
     * Deletes the given storable, using the serialized key, returing true if
     * deleted.
     *
     * @param txn optional
     * @param pipe send serialized key properties
     * @return true if deleted
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=PersistException.class)
    Pipe tryDelete(RemoteTransaction txn, Pipe pipe) throws PersistException;

    /**
     * Counts storables for this storage.
     *
     * @param fv optional
     * @param txn optional
     */
    @RemoteFailure(exception=FetchException.class)
    long queryCount(FilterValues fv, RemoteTransaction txn) throws FetchException;

    /**
     * Fetches storables for this storage.
     *
     * @param fv optional
     * @param orderBy optional
     * @param from optional
     * @param to optional
     * @param txn optional
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=FetchException.class)
    Pipe queryFetch(FilterValues fv, OrderingList orderBy, Long from, Long to,
                    RemoteTransaction txn, Pipe pipe)
        throws FetchException;

    /**
     * Fetches one storable.
     *
     * @param fv optional
     * @param txn optional
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=FetchException.class)
    Pipe queryLoadOne(FilterValues fv, RemoteTransaction txn, Pipe pipe) throws FetchException;

    /**
     * Fetches one storable.
     *
     * @param fv optional
     * @param txn optional
     */
    @Asynchronous(CallMode.REQUEST_REPLY)
    @RemoteFailure(exception=FetchException.class)
    Pipe queryTryLoadOne(FilterValues fv, RemoteTransaction txn, Pipe pipe) throws FetchException;

    /**
     * Deletes one storable.
     *
     * @param fv optional
     * @param txn optional
     */
    @RemoteFailure(exception=PersistException.class)
    void queryDeleteOne(FilterValues fv, RemoteTransaction txn)
        throws FetchException, PersistException;

    /**
     * Deletes one storable.
     *
     * @param fv optional
     * @param txn optional
     */
    @RemoteFailure(exception=PersistException.class)
    boolean queryTryDeleteOne(FilterValues fv, RemoteTransaction txn)
        throws FetchException, PersistException;

    /**
     * Deletes all matching storables.
     *
     * @param fv optional
     * @param txn optional
     */
    @RemoteFailure(exception=PersistException.class)
    void queryDeleteAll(FilterValues fv, RemoteTransaction txn)
        throws FetchException, PersistException;

    /**
     * Prints the native query to a string, returning null if no native query.
     *
     * @param fv optional
     * @param orderBy optional
     * @param indentLevel amount to indent text, zero for none
     */
    @RemoteFailure(exception=FetchException.class)
    String queryPrintNative(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException;

    /**
     * Prints the query plan to a string, returning null if plan not known.
     *
     * @param fv optional
     * @param orderBy optional
     * @param indentLevel amount to indent text, zero for none
     */
    @RemoteFailure(exception=FetchException.class)
    String queryPrintPlan(FilterValues fv, OrderingList orderBy, int indentLevel)
        throws FetchException;

    @RemoteFailure(exception=PersistException.class)
    void truncate(RemoteTransaction txn) throws PersistException;

    @RemoteFailure(exception=FetchException.class)
    Set<String> getPropertySupport(String... propertyNames) throws FetchException;
}
