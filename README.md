# pyspark-triangle-count
These are my attempts in implementation of the triangle count algorithm without using GraphFrames or GraphX in Spark.
There are two files in this repository:


Edgecentric.py computes the size of the intersection of neighbouring sets for each edge and sums the obtained counts for every vertex. This makes for a very memory hungry algorithm as every it requires to keep lists of neighbours for both vertices. Each vertex is listed d times, where d is the degree of the vertex.


Wedgecentric.py finds all unique neighbours of a vertex and creates all possible combinations of edges from two neigbouring vertices. This creates a list open and closed wedges. It can be then compared to all existing edges and the number of times we check for an candidate edge can be counted.


The data used for this was the list of ethereum transactions over a period of time and an edge is considered to be two addresses which performed a successful transaction at some point. 

This is an example of an output produced after running wedgecentric.py on a very small portion of data:

(address, number of triangles it created through transactions)
'0xa5d863a936614903c3e5c9434559b8a994fd6b2a', 152
'0x3143e1cabc3547acde8b630e0dea3b1dfebb3cb0', 145
'0x86c9981b0d85e1cd6f42b10b8305ffd88f64f55e', 115
'0x75a931567048edd4f349fa1a1cfbc4b4dca352c9', 79
'0x4df812f6064def1e5e029f1ca858777cc98d2d81', 52
