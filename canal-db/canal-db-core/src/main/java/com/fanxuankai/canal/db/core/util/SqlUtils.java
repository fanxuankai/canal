package com.fanxuankai.canal.db.core.util;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * @author fanxuankai
 */
public final class SqlUtils {
    public static void executeBatch(DataSource dataSource, List<String> sqlList) throws Exception {
        Connection conn = dataSource.getConnection();
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            try (Statement st = conn.createStatement()) {
                for (String sql : sqlList) {
                    st.addBatch(sql);
                }
                st.executeBatch();
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
            conn.close();
        }
    }
}
