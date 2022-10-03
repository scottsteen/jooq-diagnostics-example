package com.github.scottsteen;

import java.util.List;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.jupiter.api.Test;

import static com.github.scottsteen.RepeatedQueryTest.Schema.CHILD;
import static com.github.scottsteen.RepeatedQueryTest.Schema.PARENT;
import static java.util.stream.Collectors.joining;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.fail;

class RepeatedQueryTest {

  @Test
  void detectsRepeatedQueryWithExecuteListener() {
    createTables();
    insertRows(3);
    repeatQuery();

    var repeatedQueries = RepeatedQueryDetectionExecuteListener.completeTest();
    if (!repeatedQueries.isEmpty()) {
      fail("""
        Your code is functionally correct, but there were repeated queries identified!
                  
        %s""".formatted(prettyPrint(repeatedQueries)));
    }
  }

  @Test
  void detectsRepeatedQueryWithDiagnosticListener() {
    createTables();
    insertRows(3);
    repeatQuery();

    var repeatedQueries = RepeatedQueryDetectionDiagnosticListener.completeTest();

    // This should fail, but doesn't.
    // The code here does not have access to DiagnosticsDataSource.
    if (!repeatedQueries.isEmpty()) {
      fail("""
        Your code is functionally correct, but there were repeated queries identified!
                  
        %s""".formatted(prettyPrint(repeatedQueries)));
    }
  }

  private void repeatQuery() {
    var parents = dsl()
      .select(PARENT.ID)
      .from(PARENT.TABLE)
      .fetch(PARENT.ID);

    parents.forEach(p -> dsl()
      .select(CHILD.ID)
      .from(CHILD.TABLE)
      .where(CHILD.PARENT.eq(p))
      .execute());
  }

  private void insertRows(int count) {
    IntStream.range(0, count)
      .forEach(i -> {
        var parentId = dsl()
          .insertInto(PARENT.TABLE)
          .defaultValues()
          .returning(PARENT.ID)
          .fetchOne()
          .get(PARENT.ID);
        dsl()
          .insertInto(CHILD.TABLE, CHILD.PARENT)
          .values(parentId)
          .values(parentId)
          .values(parentId)
          .execute();
      });
  }

  private void createTables() {
    dsl().createTableIfNotExists(PARENT.TABLE)
      .column(PARENT.ID)
      .execute();
    dsl().createTableIfNotExists(CHILD.TABLE)
      .column(CHILD.ID)
      .column(CHILD.PARENT)
      .execute();
  }

  private static String prettyPrint(List<RepeatedQuery> repeatedQueries) {
    return repeatedQueries.stream()
      .map(Object::toString)
      .collect(joining("\n  ", "[\n  ", "\n]"));
  }

  private DSLContext dsl() {
    var configuration = new DefaultConfiguration()
      .set(dataSource())
      .set(SQLDialect.HSQLDB)
      .set(new RepeatedQueryDetectionExecuteListener())
      .set(new RepeatedQueryDetectionDiagnosticListener());
    return DSL.using(configuration);
  }

  private static DataSource dataSource() {
    var ds = new JDBCDataSource();
    ds.setUrl("jdbc:hsqldb:mem:test");
    return ds;
  }

  static class Schema {

    static final ParentTable PARENT = new ParentTable();

    static final ChildTable CHILD = new ChildTable();

    static class ParentTable {

      private final Table<Record> TABLE = table(name("parent"));
      private final Field<Integer> ID = field(name("parent", "id"), Integer.class);
    }

    static class ChildTable {

      private final Table<Record> TABLE = table(name("child"));
      private final Field<Integer> ID = field(name("child", "id"), Integer.class);
      private final Field<Integer> PARENT = field(name("child", "parent"), Integer.class);
    }
  }
}
