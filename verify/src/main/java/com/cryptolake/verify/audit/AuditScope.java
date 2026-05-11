package com.cryptolake.verify.audit;

import java.util.List;

public record AuditScope(
    long startMs,
    long endMs,
    List<String> exchanges,
    List<String> symbols,
    List<String> streams,
    String baseDir) {}
