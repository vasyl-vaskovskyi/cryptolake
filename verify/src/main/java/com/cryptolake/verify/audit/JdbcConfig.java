package com.cryptolake.verify.audit;

/** JDBC connection parameters for the PG-backed gap sources. */
public record JdbcConfig(String url, String user, String password) {}
