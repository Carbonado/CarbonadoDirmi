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

import org.cojen.dirmi.Pipe;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.qe.OrderingList;
import com.amazon.carbonado.qe.QueryHints;
import com.amazon.carbonado.qe.StandardQuery;
import com.amazon.carbonado.qe.StandardQueryFactory;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ClientQueryFactory<S extends Storable> extends StandardQueryFactory<S> {
    private final ClientStorage<S> mStorage;

    ClientQueryFactory(Class<S> type, ClientStorage<S> storage) {
        super(type);
        mStorage = storage;
    }

    protected StandardQuery<S> createQuery(Filter<S> filter,
                                           FilterValues<S> values,
                                           OrderingList<S> ordering,
                                           QueryHints hints)
        throws FetchException
    {
        return new ClientQuery<S>(this, filter, values, ordering, hints);
    }

    long queryCount(FilterValues<S> values) throws FetchException {
        return mStorage.queryCount(values);
    }

    Pipe queryFetch(FilterValues fv, OrderingList orderBy) throws FetchException {
        return mStorage.queryFetch(fv, orderBy);
    }

    Pipe queryFetch(FilterValues fv, OrderingList orderBy, long from, Long to)
        throws FetchException
    {
        return mStorage.queryFetch(fv, orderBy, from, to);
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

    ClientStorage<S> clientStorage() {
        return mStorage;
    }
}
