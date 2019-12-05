# pyspark-triangle-count
These are my attempts in implementation of the triangle count algorithm without using GraphFrames or GraphX in Spark.
There are two files in this repository:


Edgecentric.py computes the size of the intersection of neighbouring sets for each edge and sums the obtained counts for every vertex. This makes for a very memory hungry algorithm as every it requires to keep lists of neighbours for both vertices. Each vertex is listed d times, where d is the degree of the vertex.

Wedgecentric.py finds all unique neighbours of a vertex and creates all possible combinations of edges from two neigbouring vertices. This creates a list open and closed wedges. It can be then compared to all existing edges and the number of times we check for an candidate edge can be counted.
