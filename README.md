# Performance-Debug-Benchmarks
Benchmarks for performance debugging.
1. An example of PageRank in Spark:
  PageRank algorithm will demonstrate significant data skew on nodes that have many links.
  pagerank_data is an example with data skew in the simplest form.
  hollins.data is a real dataset that includes all the web addresses associated with Hollins university and their linking relationships.
  hollins_num.data only contains the format: url_1 url_2
2. Word Count in Spark:
  Lines with many words would be slower.
  To demonstrate the problem more obviously, we can make the program wait on each word for 10 milliseconds.
3. Weather Analysis

# See Report
