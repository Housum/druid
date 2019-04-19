/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.pool;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;

import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.filter.FilterChainImpl;
import com.alibaba.druid.pool.DruidPooledPreparedStatement.PreparedStatementKey;
import com.alibaba.druid.pool.PreparedStatementPool.MethodType;
import com.alibaba.druid.proxy.jdbc.TransactionInfo;
import com.alibaba.druid.support.logging.Log;
import com.alibaba.druid.support.logging.LogFactory;

/**
 * 被池话的连接 该连接与物理连接的不同在于 其方法是针对连接池的方法执行 比如
 * close 只是将其回收到到了连接池中了 连接池返回的就是该类
 * @author wenshao [szujobs@hotmail.com]
 */
public class DruidPooledConnection extends PoolableWrapper implements javax.sql.PooledConnection, Connection {
    private final static Log                   LOG                  = LogFactory.getLog(DruidPooledConnection.class);
    public static final  int                   MAX_RECORD_SQL_COUNT = 10;
    protected            Connection            conn;
    //内存储存了物理连接
    protected volatile   DruidConnectionHolder holder;
    protected            TransactionInfo       transactionInfo;
    private final        boolean               dupCloseLogEnable;
    protected volatile   boolean               traceEnable          = false;
    private   volatile   boolean               disable              = false;
    protected volatile   boolean               closed               = false;
    protected final      Thread                ownerThread;
    private              long                  connectedTimeMillis;
    private              long                  connectedTimeNano;
    private volatile     boolean               running              = false;
    private volatile     boolean               abandoned            = false;
    protected            StackTraceElement[]   connectStackTrace;
    protected            Throwable             disableError         = null;
    final                ReentrantLock         lock;

    public DruidPooledConnection(DruidConnectionHolder holder){
        super(holder.getConnection());

        this.conn = holder.getConnection();
        this.holder = holder;
        this.lock = holder.lock;
        dupCloseLogEnable = holder.getDataSource().isDupCloseLogEnable();
        ownerThread = Thread.currentThread();
        //连接时间
        connectedTimeMillis = System.currentTimeMillis();
    }

    public long getConnectedTimeMillis() {
        return connectedTimeMillis;
    }

    public Thread getOwnerThread() {
        return ownerThread;
    }

    public StackTraceElement[] getConnectStackTrace() {
        return connectStackTrace;
    }

    public void setConnectStackTrace(StackTraceElement[] connectStackTrace) {
        this.connectStackTrace = connectStackTrace;
    }

    public long getConnectedTimeNano() {
        return connectedTimeNano;
    }

    public void setConnectedTimeNano() {
        if (connectedTimeNano <= 0) {
            this.setConnectedTimeNano(System.nanoTime());
        }
    }

    public void setConnectedTimeNano(long connectedTimeNano) {
        this.connectedTimeNano = connectedTimeNano;
    }

    public boolean isTraceEnable() {
        return traceEnable;
    }

    public void setTraceEnable(boolean traceEnable) {
        this.traceEnable = traceEnable;
    }

    public SQLException handleException(Throwable t) throws SQLException {
        return handleException(t, null);
    }

    //出现错误
    public SQLException handleException(Throwable t, String sql) throws SQLException {
        final DruidConnectionHolder holder = this.holder;

        //
        if (holder != null) {
            DruidAbstractDataSource dataSource = holder.getDataSource();
            dataSource.handleConnectionException(this, t, sql);
        }

        if (t instanceof SQLException) {
            throw (SQLException) t;
        }

        throw new SQLException("Error", t);
    }

    public boolean isOracle() {
        return holder.getDataSource().isOracle();
    }

    /**
     * 对池化的prepareStatement进行回收
     * @param stmt
     * @throws SQLException
     */
    public void closePoolableStatement(DruidPooledPreparedStatement stmt) throws SQLException {
        PreparedStatement rawStatement = stmt.getRawPreparedStatement();

        if (holder == null) {
            return;
        }

        if (stmt.isPooled()) {
            try {
                //调用native api 清理参数
                rawStatement.clearParameters();
            } catch (SQLException ex) {
                this.handleException(ex, null);
                if (rawStatement.getConnection().isClosed()) {
                    return;
                }

                LOG.error("clear parameter error", ex);
            }
        }

        PreparedStatementHolder stmtHolder = stmt.getPreparedStatementHolder();
        //减少使用数量
        stmtHolder.decrementInUseCount();
        if (stmt.isPooled() && holder.isPoolPreparedStatements() && stmt.exceptionCount == 0) {
            holder.getStatementPool().put(stmtHolder);

            stmt.clearResultSet();
            holder.removeTrace(stmt);

            stmtHolder.setFetchRowPeak(stmt.getFetchRowPeak());

            stmt.setClosed(true); // soft set close
        } else if (stmt.isPooled() && holder.isPoolPreparedStatements()) {
            // the PreparedStatement threw an exception
            stmt.clearResultSet();
            holder.removeTrace(stmt);

            holder.getStatementPool()
                    .remove(stmtHolder);
        } else {
            try {
                //Connection behind the statement may be in invalid state, which will throw a SQLException.
                //In this case, the exception is desired to be properly handled to remove the unusable connection from the pool.
                stmt.closeInternal();
            } catch (SQLException ex) {
                this.handleException(ex, null);
                throw ex;
            } finally {
                holder.getDataSource().incrementClosedPreparedStatementCount();
            }
        }
    }

    public DruidConnectionHolder getConnectionHolder() {
        return holder;
    }

    @Override
    public Connection getConnection() {
        if (!holder.underlyingAutoCommit) {
            createTransactionInfo();
        }

        return conn;
    }

    public void disable() {
        disable(null);
    }

    public void disable(Throwable error) {
        if (this.holder != null) {
            this.holder.clearStatementCache();
        }

        this.traceEnable = false;
        this.holder = null;
        this.transactionInfo = null;
        this.disable = true;
        this.disableError = error;
    }

    public boolean isDisable() {
        return disable;
    }

    /**
     * 这里的关闭并非是正在的关闭 这里主要的做的还是回收连接 将连接放入到
     * 连接池的connections中
     */
    @Override
    public void close() throws SQLException {
        //该连接不能在被调用(后面关闭之后将会设置为true)
        if (this.disable) {
            return;
        }

        //检测是否重复关闭
        DruidConnectionHolder holder = this.holder;
        if (holder == null) {
            if (dupCloseLogEnable) {
                LOG.error("dup close");
            }
            return;
        }

        DruidAbstractDataSource dataSource = holder.getDataSource();
        boolean isSameThread = this.getOwnerThread() == Thread.currentThread();

        //如果关闭该连接的不是持有它的线程 那么设置属性为
        if (!isSameThread) {
            dataSource.setAsyncCloseConnectionEnable(true);
        }

        //关闭
        if (dataSource.isAsyncCloseConnectionEnable()) {
            //因为不是同一个线程的 所以和后面的代码唯一区别就是使用了lock(该lock也是DruidConnectionHolder的锁)
            syncClose();
            return;
        }

        //通知监听事件
        for (ConnectionEventListener listener : holder.getConnectionEventListeners()) {
            listener.connectionClosed(new ConnectionEvent(this));
        }

        
        List<Filter> filters = dataSource.getProxyFilters();
        if (filters.size() > 0) {
            //数据源回收连接  调用filter
            FilterChainImpl filterChain = new FilterChainImpl(dataSource);
            filterChain.dataSource_recycle(this);
        } else {
            //回收
            recycle();
        }

        this.disable = true;
    }

    public void syncClose() throws SQLException {
        lock.lock();
        try {
            if (this.disable) {
                return;
            }

            DruidConnectionHolder holder = this.holder;
            if (holder == null) {
                if (dupCloseLogEnable) {
                    LOG.error("dup close");
                }
                return;
            }

            for (ConnectionEventListener listener : holder.getConnectionEventListeners()) {
                listener.connectionClosed(new ConnectionEvent(this));
            }

            DruidAbstractDataSource dataSource = holder.getDataSource();
            List<Filter> filters = dataSource.getProxyFilters();
            if (filters.size() > 0) {
                FilterChainImpl filterChain = new FilterChainImpl(dataSource);
                filterChain.dataSource_recycle(this);
            } else {
                recycle();
            }

            this.disable = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 回收连接  底层的做法是将物理连接给回收到了连接池中
     */
    public void recycle() throws SQLException {
        if (this.disable) {
            return;
        }

        DruidConnectionHolder holder = this.holder;
        if (holder == null) {
            if (dupCloseLogEnable) {
                LOG.error("dup close");
            }
            return;
        }

        if (!this.abandoned) {
            DruidAbstractDataSource dataSource = holder.getDataSource();
            //调用数据源的方法 将此连接回收
            dataSource.recycle(this);
        }

        //将当前对象的所有属性情况 help GC,同时也是为了防止关闭之后其他线程还能够使用
        this.holder = null;
        conn = null;
        transactionInfo = null;
        closed = true;
    }

    // ////////////////////

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        //作为PSCache的key 底层重写了hashCode和equal方法
        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.M1);

        //返回是否配置了PSCache
        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        //如果配置了的话 那么返回缓存中的PSCache
        if (poolPreparedStatements) {
            //这里虽然没有加锁 但是是不会有并发问题的 因为DruidPooledConnection是线程内执行 不会散到其他的线程
            //如果在一个连接中 命中了超过两次 那么将会新建一个连接 另外一个连接使用池中的对象
            stmtHolder = holder.getStatementPool().get(key);
        }

        //如果缓存中不存在 生成prepareStatement
        if (stmtHolder == null) {
            try {
                //conn.prepareStatement(sql)创建的是被代理过的prepareStatement
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new DruidPooledPreparedStatement(this, stmtHolder);

        //保存执行的statement
        holder.addTrace(rtnVal);

        return rtnVal;
    }

    private void initStatement(PreparedStatementHolder stmtHolder) throws SQLException {
        //使用次数加1
        stmtHolder.incrementInUseCount();
        holder.getDataSource().initStatement(this, stmtHolder.statement);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                      throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.M2, resultSetType,
                                                            resultSetConcurrency);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, resultSetType,
                                                                                    resultSetConcurrency));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new DruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.M3, resultSetType,
                                                            resultSetConcurrency, resultSetHoldability);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, resultSetType,
                                                                                    resultSetConcurrency,
                                                                                    resultSetHoldability));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new DruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkState();

        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.M4, columnIndexes);
        PreparedStatementHolder stmtHolder = null;

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, columnIndexes));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new DruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkState();

        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.M5, columnNames);
        PreparedStatementHolder stmtHolder = null;

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, columnNames));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new DruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkState();

        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.M6, autoGeneratedKeys);
        PreparedStatementHolder stmtHolder = null;

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareStatement(sql, autoGeneratedKeys));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledPreparedStatement rtnVal = new DruidPooledPreparedStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    // ////////////////////

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.Precall_1);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareCall(sql));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledCallableStatement rtnVal = new DruidPooledCallableStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.Precall_2, resultSetType,
                                                            resultSetConcurrency, resultSetHoldability);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key, conn.prepareCall(sql, resultSetType,
                                                                               resultSetConcurrency,
                                                                               resultSetHoldability));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledCallableStatement rtnVal = new DruidPooledCallableStatement(this, stmtHolder);

        holder.addTrace(rtnVal);

        return rtnVal;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkState();

        PreparedStatementHolder stmtHolder = null;
        PreparedStatementKey key = new PreparedStatementKey(sql, getCatalog(), MethodType.Precall_3, resultSetType,
                                                            resultSetConcurrency);

        boolean poolPreparedStatements = holder.isPoolPreparedStatements();

        if (poolPreparedStatements) {
            stmtHolder = holder.getStatementPool().get(key);
        }

        if (stmtHolder == null) {
            try {
                stmtHolder = new PreparedStatementHolder(key,
                                                         conn.prepareCall(sql, resultSetType, resultSetConcurrency));
                holder.getDataSource().incrementPreparedStatementCount();
            } catch (SQLException ex) {
                handleException(ex, sql);
            }
        }

        initStatement(stmtHolder);

        DruidPooledCallableStatement rtnVal = new DruidPooledCallableStatement(this, stmtHolder);
        holder.addTrace(rtnVal);

        return rtnVal;
    }

    // //////////////////// statement没有缓存能力 直接每次都创建新的对象

    @Override
    public Statement createStatement() throws SQLException {
        checkState();

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException ex) {
            handleException(ex, null);
        }

        holder.getDataSource().initStatement(this, stmt);

        DruidPooledStatement poolableStatement = new DruidPooledStatement(this, stmt);
        holder.addTrace(poolableStatement);

        return poolableStatement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                           throws SQLException {
        checkState();

        Statement stmt = null;
        try {
            stmt = conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException ex) {
            handleException(ex, null);
        }

        holder.getDataSource().initStatement(this, stmt);

        DruidPooledStatement poolableStatement = new DruidPooledStatement(this, stmt);
        holder.addTrace(poolableStatement);

        return poolableStatement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkState();

        Statement stmt = null;
        try {
            stmt = conn.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException ex) {
            handleException(ex, null);
        }

        holder.getDataSource().initStatement(this, stmt);

        DruidPooledStatement poolableStatement = new DruidPooledStatement(this, stmt);
        holder.addTrace(poolableStatement);

        return poolableStatement;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkState();

        return conn.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkState();

        boolean useLocalSessionState = holder.getDataSource().isUseLocalSessionState();

        if (useLocalSessionState) {
            if (autoCommit == holder.underlyingAutoCommit) {
                return;
            }
        }

        try {
            conn.setAutoCommit(autoCommit);
            holder.setUnderlyingAutoCommit(autoCommit);
        } catch (SQLException ex) {
            handleException(ex, null);
        }
    }

    protected void transactionRecord(String sql) throws SQLException {
        if (transactionInfo == null && (!conn.getAutoCommit())) {
            DruidAbstractDataSource dataSource = holder.getDataSource();
            dataSource.incrementStartTransactionCount();
            transactionInfo = new TransactionInfo(dataSource.createTransactionId());
        }

        if (transactionInfo != null) {
            List<String> sqlList = transactionInfo.getSqlList();
            if (sqlList.size() < MAX_RECORD_SQL_COUNT) {
                sqlList.add(sql);
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkState();

        return conn.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        checkState();

        DruidAbstractDataSource dataSource = holder.getDataSource();
        dataSource.incrementCommitCount();

        try {
            conn.commit();
        } catch (SQLException ex) {
            handleException(ex, null);
        } finally {
            handleEndTransaction(dataSource, null);
        }
    }

    public TransactionInfo getTransactionInfo() {
        return transactionInfo;
    }

    protected void createTransactionInfo() {
        DruidAbstractDataSource dataSource = holder.getDataSource();
        dataSource.incrementStartTransactionCount();
        transactionInfo = new TransactionInfo(dataSource.createTransactionId());
    }

    @Override
    public void rollback() throws SQLException {
        if (transactionInfo == null) {
            return;
        }

        if (holder == null) {
            return;
        }

        DruidAbstractDataSource dataSource = holder.getDataSource();
        dataSource.incrementRollbackCount();

        try {
            conn.rollback();
        } catch (SQLException ex) {
            handleException(ex, null);
        } finally {
            handleEndTransaction(dataSource, null);
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkState();

        try {
            return conn.setSavepoint(name);
        } catch (SQLException ex) {
            handleException(ex, null);
            return null; // never arrive
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (holder == null) {
            return;
        }

        DruidAbstractDataSource dataSource = holder.getDataSource();
        dataSource.incrementRollbackCount();

        try {
            conn.rollback(savepoint);
        } catch (SQLException ex) {
            handleException(ex, null);
        } finally {
            handleEndTransaction(dataSource, savepoint);
        }
    }

    private void handleEndTransaction(DruidAbstractDataSource dataSource, Savepoint savepoint) {
        if (transactionInfo != null && savepoint == null) {
            transactionInfo.setEndTimeMillis();

            long transactionMillis = transactionInfo.getEndTimeMillis() - transactionInfo.getStartTimeMillis();
            dataSource.getTransactionHistogram().record(transactionMillis);

            dataSource.logTransaction(transactionInfo);

            transactionInfo = null;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkState();
        try {
            conn.releaseSavepoint(savepoint);
        } catch (SQLException ex) {
            handleException(ex, null);
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        checkState();

        try {
            return conn.createClob();
        } catch (SQLException ex) {
            handleException(ex, null);
            return null; // never arrive
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        if (holder == null) {
            return true;
        }

        return closed || disable;
    }

    public boolean isAbandonded() {
        return this.abandoned;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkState();

        if (!holder.underlyingAutoCommit) {
            createTransactionInfo();
        }

        return conn.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkState();

        boolean useLocalSessionState = holder.getDataSource().isUseLocalSessionState();
        if (useLocalSessionState) {
            if (readOnly == holder.isUnderlyingReadOnly()) {
                return;
            }
        }

        try {
            conn.setReadOnly(readOnly);
        } catch (SQLException ex) {
            handleException(ex, null);
        }

        holder.setUnderlyingReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkState();

        return conn.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkState();

        try {
            conn.setCatalog(catalog);
        } catch (SQLException ex) {
            handleException(ex, null);
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        checkState();

        return conn.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkState();

        boolean useLocalSessionState = holder.getDataSource().isUseLocalSessionState();
        if (useLocalSessionState) {
            if (level == holder.getUnderlyingTransactionIsolation()) {
                return;
            }
        }

        try {
            conn.setTransactionIsolation(level);
        } catch (SQLException ex) {
            handleException(ex, null);
        }
        holder.setUnderlyingTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkState();

        return holder.getUnderlyingTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkState();

        return conn.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkState();
        try {
            conn.clearWarnings();
        } catch (SQLException ex) {
            handleException(ex, null);
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkState();

        return conn.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkState();

        conn.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkState();

        boolean useLocalSessionState = holder.getDataSource().isUseLocalSessionState();
        if (useLocalSessionState) {
            if (holdability == holder.getUnderlyingHoldability()) {
                return;
            }
        }

        conn.setHoldability(holdability);
        holder.setUnderlyingHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkState();

        if (!holder.underlyingAutoCommit) {
            createTransactionInfo();
        }

        return conn.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkState();

        try {
            return conn.setSavepoint();
        } catch (SQLException ex) {
            handleException(ex, null);
            return null;
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkState();

        return conn.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkState();

        return conn.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkState();

        return conn.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkState();

        return conn.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (holder == null) {
            throw new SQLClientInfoException();
        }

        conn.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        if (holder == null) {
            throw new SQLClientInfoException();
        }

        conn.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkState();

        return conn.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkState();

        return conn.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkState();

        return conn.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkState();

        return conn.createStruct(typeName, attributes);
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        if (holder == null) {
            throw new IllegalStateException();
        }

        holder.getConnectionEventListeners().add(listener);
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        if (holder == null) {
            throw new IllegalStateException();
        }

        holder.getConnectionEventListeners().remove(listener);
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        if (holder == null) {
            throw new IllegalStateException();
        }

        holder.getStatementEventListeners().add(listener);
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        if (holder == null) {
            throw new IllegalStateException();
        }

        holder.getStatementEventListeners().remove(listener);
    }

    public Throwable getDisableError() {
        return disableError;
    }

    /**
     * 检查连接的状态
     */
    public void checkState() throws SQLException {
        final boolean asyncCloseEnabled;
        if (holder != null) {
            asyncCloseEnabled = holder.getDataSource().isAsyncCloseConnectionEnable();
        } else {
            asyncCloseEnabled = false;
        }
        
        if (asyncCloseEnabled) {
            lock.lock();
            try {
                checkStateInternal();
            } finally {
                lock.unlock();
            }
        } else {
            checkStateInternal();
        }
    }
    
    private void checkStateInternal() throws SQLException {
        if (closed) {
            if (disableError != null) {
                throw new SQLException("connection closed", disableError);
            } else {
                throw new SQLException("connection closed");
            }
        }

        if (disable) {
            if (disableError != null) {
                throw new SQLException("connection disabled", disableError);
            } else {
                throw new SQLException("connection disabled");
            }
        }

        if (holder == null) {
            if (disableError != null) {
                throw new SQLException("connection holder is null", disableError);
            } else {
                throw new SQLException("connection holder is null");
            }
        }
    }

    public String toString() {
        if (conn != null) {
            return conn.toString();
        } else {
            return "closed-conn-" + System.identityHashCode(this);
        }
    }

    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    final void beforeExecute() {
        final DruidConnectionHolder holder = this.holder;
        if (holder != null && holder.dataSource.removeAbandoned) {
            running = true;
        }
    }

    final void afterExecute() {
        final DruidConnectionHolder holder = this.holder;
        if (holder != null) {
            DruidAbstractDataSource dataSource = holder.dataSource;
            if (dataSource.removeAbandoned) {
                running = false;
                holder.lastActiveTimeMillis = System.currentTimeMillis();
            }
            dataSource.onFatalError = false;
        }
    }

    boolean isRunning() {
        return running;
    }

    public void abandond() {
        this.abandoned = true;
    }
    
    /**
     * @since 1.0.17
     */
    public long getPhysicalConnectNanoSpan() {
        return this.holder.getCreateNanoSpan();
    }
    
    /**
     * @since 1.0.17
     */
    public long getPhysicalConnectionUsedCount() {
        return this.holder.getUseCount();
    }
    
    /**
     * @since 1.0.17
     */
    public long getConnectNotEmptyWaitNanos() {
        return this.holder.getLastNotEmptyWaitNanos();
    }

    /**
     * @since  1.0.28
     */
    public Map<String, Object> getVariables() {
        return this.holder.variables;
    }

    /**
     * @since  1.0.28
     */
    public Map<String, Object> getGloablVariables() {
        return this.holder.globleVariables;
    }
}
