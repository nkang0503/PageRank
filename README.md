# Performance-Debug-Benchmarks
Benchmarks for performance debugging.
1. An example of PageRank in Spark:
  PageRank algorithm will demonstrate significant data skew on nodes that have many links.
2. Word Count in Spark:
  lines with many words would be slower
  To demonstrate the problem more obviously, we can make the program wait on each word for 10 milliseconds.
