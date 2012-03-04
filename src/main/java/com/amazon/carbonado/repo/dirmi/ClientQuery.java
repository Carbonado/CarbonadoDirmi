/*
 * Copyright 2008-2012 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.qe.OrderingList;
import com.amazon.carbonado.qe.QueryExecutor;
import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.QueryFactory;
import com.amazon.carbonado.qe.QueryHints;
import com.amazon.carbonado.qe.StandardQuery;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ClientQuery<S extends Storable> extends StandardQuery<S> implements QueryExecutorFactory<S> {
    final ClientQueryFactory<S> mQueryFactory;

    ClientQuery(ClientQueryFactory<S> queryFactory,
                Filter<S> filter,
                FilterValues<S> values,
                OrderingList<S> ordering,
                QueryHints hints)
    {
        super(filter, values, ordering, hints);
        mQueryFactory = queryFactory;
    }

    @Override
    public S loadOne() throws FetchException {
        return loadOne(null);
    }

    @Override
    public S loadOne(Controller controller) throws FetchException {
        return mQueryFactory.clientStorage().queryLoadOne(getFilterValues(), controller);
    }

    @Override
    public S tryLoadOne() throws FetchException {
        return tryLoadOne(null);
    }

    @Override
    public S tryLoadOne(Controller controller) throws FetchException {
        return mQueryFactory.clientStorage().queryTryLoadOne(getFilterValues(), controller);
    }

    @Override
    public void deleteOne() throws PersistException {
        deleteOne(null);
    }

    @Override
    public void deleteOne(Controller controller) throws PersistException {
        mQueryFactory.clientStorage().queryDeleteOne(getFilterValues(), controller);
    }

    @Override
    public boolean tryDeleteOne() throws PersistException {
        return tryDeleteOne(null);
    }

    @Override
    public boolean tryDeleteOne(Controller controller) throws PersistException {
        return mQueryFactory.clientStorage().queryTryDeleteOne(getFilterValues(), controller);
    }

    @Override
    public void deleteAll() throws PersistException {
        deleteAll(null);
    }

    @Override
    public void deleteAll(Controller controller) throws PersistException {
        mQueryFactory.clientStorage().queryDeleteAll(getFilterValues(), controller);
    }

    @Override
    protected Transaction enterTransaction(IsolationLevel level) {
        return null;
    }

    @Override
    protected QueryFactory<S> queryFactory() {
        return mQueryFactory;
    }

    @Override
    protected QueryExecutorFactory<S> executorFactory() {
        return this;
    }

    @Override
    protected StandardQuery<S> newInstance(FilterValues<S> values,
                                           OrderingList<S> ordering,
                                           QueryHints hints)
    {
        return new ClientQuery<S>(mQueryFactory, values.getFilter(), values, ordering, hints);
    }

    @Override
    public QueryExecutor<S> executor(Filter<S> filter,
                                     OrderingList<S> ordering,
                                     QueryHints hints)
        throws RepositoryException
    {
        return new Executor(filter, ordering);
    }

    private class Executor implements QueryExecutor<S> {
        private final Filter<S> mFilter;
        private final OrderingList<S> mOrdering;

        Executor(Filter<S> filter, OrderingList<S> ordering) {
            mFilter = filter;
            mOrdering = ordering;
        }

        public Class<S> getStorableType() {
            return ClientQuery.this.getStorableType();
        }

        @Override
        public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
            return fetch(values, null);
        }

        @Override
        public Cursor<S> fetch(FilterValues<S> values, Query.Controller controller)
            throws FetchException
        {
            return mQueryFactory.clientStorage()
                .queryFetch(values, mOrdering, null, null, controller);
        }

        @Override
        public Cursor<S> fetchSlice(FilterValues<S> values, long from, Long to)
            throws FetchException
        {
            return fetchSlice(values, from, to, null);
        }

        @Override
        public Cursor<S> fetchSlice(FilterValues<S> values, long from, Long to,
                                    Query.Controller controller)
            throws FetchException
        {
            return mQueryFactory.clientStorage()
                .queryFetch(values, mOrdering, from, to, controller);
        }

        @Override
        public long count(FilterValues<S> values) throws FetchException {
            return count(values, null);
        }

        @Override
        public long count(FilterValues<S> values, Query.Controller controller)
            throws FetchException
        {
            return mQueryFactory.clientStorage().queryCount(values, controller);
        }

        @Override
        public Filter<S> getFilter() {
            return mFilter;
        }

        @Override
        public OrderingList<S> getOrdering() {
            return mOrdering;
        }

        @Override
        public boolean printNative(Appendable app,
                                   int indentLevel,
                                   FilterValues<S> values)
            throws IOException
        {
            try {
                String str = mQueryFactory.clientStorage()
                    .queryPrintNative(values, mOrdering, indentLevel);
                if (str == null) {
                    return false;
                }
                app.append(str);
                return true;
            } catch (FetchException e) {
                return false;
            }
        }

        @Override
        public boolean printPlan(Appendable app,
                                 int indentLevel,
                                 FilterValues<S> values)
            throws IOException
        {
            try {
                String str = mQueryFactory.clientStorage()
                    .queryPrintPlan(values, mOrdering, indentLevel);
                if (str == null) {
                    return false;
                }
                app.append(str);
                return true;
            } catch (FetchException e) {
                return false;
            }
        }
    }
}
