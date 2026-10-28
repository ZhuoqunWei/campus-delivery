package dev.campusdelivery.common;

import java.sql.SQLException;

/** Maps PostgreSQL SQLSTATEs (including our custom ones raised from triggers) to API errors. */
public final class PgErrors {

    public static final String ILLEGAL_TRANSITION = "CD001";
    public static final String IMMUTABLE_COLUMN = "CD002";
    public static final String UNIQUE_VIOLATION = "23505";
    public static final String CHECK_VIOLATION = "23514";
    public static final String DEADLOCK = "40P01";

    private PgErrors() {}

    public static String sqlState(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof SQLException sql && sql.getSQLState() != null) {
                return sql.getSQLState();
            }
            cur = cur.getCause();
        }
        return null;
    }

    public static String message(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof SQLException sql) {
                return sql.getMessage();
            }
            cur = cur.getCause();
        }
        return t.getMessage();
    }

    public static boolean is(Throwable t, String state) {
        return state.equals(sqlState(t));
    }
}
