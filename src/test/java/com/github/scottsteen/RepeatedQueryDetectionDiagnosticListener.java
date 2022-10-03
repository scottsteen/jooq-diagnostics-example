package com.github.scottsteen;

import java.util.List;
import org.jooq.DiagnosticsContext;
import org.jooq.impl.DefaultDiagnosticsListener;

public final class RepeatedQueryDetectionDiagnosticListener extends DefaultDiagnosticsListener {

  private static final int REPEAT_THRESHOLD = 1;
  private static final ThreadLocal<Queries> QUERIES = ThreadLocal.withInitial(() -> new Queries(REPEAT_THRESHOLD));

  @Override
  public void repeatedStatements(DiagnosticsContext ctx) {
    QUERIES.get().recordQuery(ctx.normalisedStatement());
  }

  static List<RepeatedQuery> completeTest() {
    try {
      return QUERIES.get().repeatedQueries();
    } finally {
      QUERIES.remove();
    }
  }
}
