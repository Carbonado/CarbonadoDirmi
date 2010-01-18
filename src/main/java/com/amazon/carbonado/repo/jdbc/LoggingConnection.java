/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.jdbc;

import java.sql.*;

import org.apache.commons.logging.Log;

/**
 * Connection returned by LoggingDataSource.
 *
 * @author Brian S O'Neill
 */
class LoggingConnection implements Connection {
    private final Log mLog;
    private final Connection mCon;

    LoggingConnection(Log log, Connection con) {
        mLog = log;
        mCon = con;
    }

    public Statement createStatement() throws SQLException {
        return new LoggingStatement(mLog, this, mCon.createStatement());
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException
    {
        return new LoggingStatement
            (mLog, this, mCon.createStatement(resultSetType, resultSetConcurrency));
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability)
        throws SQLException
    {
        return new LoggingStatement(mLog, this,
                                    mCon.createStatement(resultSetType, resultSetConcurrency,
                                                         resultSetHoldability));
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new LoggingPreparedStatement
            (mLog, this, mCon.prepareStatement(sql), sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency)
        throws SQLException
    {
        return new LoggingPreparedStatement
            (mLog, this, mCon.prepareStatement(sql, resultSetType, resultSetConcurrency), sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
        throws SQLException
    {
        return new LoggingPreparedStatement
            (mLog, this, mCon.prepareStatement(sql, resultSetType,
                                               resultSetConcurrency, resultSetHoldability), sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException
    {
        return new LoggingPreparedStatement
            (mLog, this, mCon.prepareStatement(sql, autoGeneratedKeys), sql);
    }

    public PreparedStatement prepareStatement(String sql, int columnIndexes[])
        throws SQLException
    {
        return new LoggingPreparedStatement
            (mLog, this, mCon.prepareStatement(sql, columnIndexes), sql);
    }

    public PreparedStatement prepareStatement(String sql, String columnNames[])
        throws SQLException
    {
        return new LoggingPreparedStatement
            (mLog, this, mCon.prepareStatement(sql, columnNames), sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return new LoggingCallableStatement(mLog, this, mCon.prepareCall(sql), sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency)
        throws SQLException
    {
        return new LoggingCallableStatement
            (mLog, this, mCon.prepareCall(sql, resultSetType, resultSetConcurrency), sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability)
        throws SQLException
    {
        return new LoggingCallableStatement
            (mLog, this, mCon.prepareCall(sql, resultSetType,
                                          resultSetConcurrency, resultSetHoldability), sql);
    }

    public String nativeSQL(String sql) throws SQLException {
        return mCon.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        mLog.debug("Connection.setAutoCommit(" + autoCommit + ')');
        mCon.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return mCon.getAutoCommit();
    }

    public void commit() throws SQLException {
        mLog.debug("Connection.commit()");
        mCon.commit();
    }

    public void rollback() throws SQLException {
        mLog.debug("Connection.rollback()");
        mCon.rollback();
    }

    public void close() throws SQLException {
        mLog.debug("Connection.close()");
        mCon.close();
    }

    public boolean isClosed() throws SQLException {
        return mCon.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        mLog.debug("Connection.getMetaData()");
        return mCon.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        mCon.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return mCon.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        mCon.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return mCon.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        String levelStr;
        switch (level) {
        default:
            levelStr = String.valueOf(level);
            break;
        case Connection.TRANSACTION_NONE:
            levelStr = "TRANSACTION_NONE";
            break;
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            levelStr = "TRANSACTION_READ_UNCOMMITTED";
            break;
        case Connection.TRANSACTION_READ_COMMITTED:
            levelStr = "TRANSACTION_READ_COMMITTED";
            break;
        case Connection.TRANSACTION_REPEATABLE_READ:
            levelStr = "TRANSACTION_REPEATABLE_READ";
            break;
        case Connection.TRANSACTION_SERIALIZABLE:
            levelStr = "TRANSACTION_SERIALIZABLE";
            break;
        }

        mLog.debug("Connection.setTransactionIsolation(" + levelStr + ')');
        mCon.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return mCon.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return mCon.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        mCon.clearWarnings();
    }

    public java.util.Map<String,Class<?>> getTypeMap() throws SQLException {
        return mCon.getTypeMap();
    }

    public void setTypeMap(java.util.Map<String,Class<?>> map) throws SQLException {
        mCon.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        mCon.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return mCon.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        mLog.debug("Connection.setSavepoint()");
        return mCon.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        mLog.debug("Connection.setSavepoint(name)");
        return mCon.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        mLog.debug("Connection.rollback(savepoint)");
        mCon.rollback(savepoint);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        mLog.debug("Connection.releaseSavepoint(savepoint)");
        mCon.releaseSavepoint(savepoint);
    }

    /**
     * @since 1.2
     */
    public Clob createClob() throws SQLException {
        return mCon.createClob();
    }

    /**
     * @since 1.2
     */
    public Blob createBlob() throws SQLException {
        return mCon.createBlob();
    }
    
    /**
     * @since 1.2
     */
    public NClob createNClob() throws SQLException {
        return mCon.createNClob();
    }

    /**
     * @since 1.2
     */
    public SQLXML createSQLXML() throws SQLException {
        return mCon.createSQLXML();
    }

    /**
     * @since 1.2
     */
    public boolean isValid(int timeout) throws SQLException {
        return mCon.isValid(timeout);
    }

    /**
     * @since 1.2
     */
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        mCon.setClientInfo(name, value);
    }
        
    /**
     * @since 1.2
     */
    public void setClientInfo(java.util.Properties properties) throws SQLClientInfoException {
        mCon.setClientInfo(properties);
    }

    /**
     * @since 1.2
     */
    public String getClientInfo(String name) throws SQLException {
        return mCon.getClientInfo(name);
    }

    /**
     * @since 1.2
     */
    public java.util.Properties getClientInfo() throws SQLException {
        return mCon.getClientInfo();
    }

    /**
     * @since 1.2
     */
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return mCon.createArrayOf(typeName, elements);
    }

    /**
     * @since 1.2
     */
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return mCon.createStruct(typeName, attributes);
    }

    /**
     * @since 1.2
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * @since 1.2
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}