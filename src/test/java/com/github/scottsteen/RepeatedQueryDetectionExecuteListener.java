package com.github.scottsteen;

import java.io.Serial;
import java.util.List;
import org.jooq.ExecuteContext;
import org.jooq.impl.DefaultExecuteListener;

import static org.jooq.ExecuteType.READ;

public final class RepeatedQueryDetectionExecuteListener extends DefaultExecuteListener {

  @Serial
  private static final long serialVersionUID = 1L;

  private static final int REPEAT_THRESHOLD = 1;
  private static final ThreadLocal<Queries> QUERIES = ThreadLocal.withInitial(() -> new Queries(REPEAT_THRESHOLD));

  @Override
  public void executeStart(ExecuteContext ctx) {
    if (ctx.type() == READ) {
      QUERIES.get().recordQuery(ctx.sql());
    }
  }

  static List<RepeatedQuery> completeTest() {
    try {
      return QUERIES.get().repeatedQueries();
    } finally {
      QUERIES.remove();
    }
  }
}
