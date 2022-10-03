package com.github.scottsteen;

record RepeatedQuery(String sql, int count) {

  @Override
  public String toString() {
    return "'%s' seen %d times".formatted(sql, count);
  }
}
