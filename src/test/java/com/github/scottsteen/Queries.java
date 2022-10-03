package com.github.scottsteen;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class Queries {

  private final Map<String, Integer> queries = new HashMap<>();
  private final int threshold;

  Queries(int threshold) {
    this.threshold = threshold;
  }

  void recordQuery(String query) {
    queries.merge(query, 1, Integer::sum);
  }

  List<RepeatedQuery> repeatedQueries() {
    return queries.entrySet().stream()
      .filter(e -> e.getValue() > threshold)
      .map(e -> new RepeatedQuery(e.getKey(), e.getValue()))
      .toList();
  }
}
