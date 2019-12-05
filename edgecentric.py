
import pyspark

# SparkContext.setSystemProperty('spark.executor.memory', '2g')
sc = pyspark.SparkContext()

def is_good_line(line):
    # filter lines in the input file
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False

        float(fields[-1])
        return True
    except:
        return False

def degree_filter(t):
    # filter the degree of a vertex to consider
    if len(t[1][1]) > 500 or len(t[1][1]) < 2:
        return False
    return True

lines = sc.textFile("/data/ethereum/transactionSmall/part-00019")

clean_lines = lines.filter(is_good_line)

# create key-value pairs: (vertex1,vertex2)
edges = clean_lines.map(lambda l: (l.split(',')[1], l.split(',')[2]))

# create rdd that has key-value pairs
# (vertex1, [nb1,nb2,nb3...]), where nbi is neighbour i of vertex1
# list(set(list - needed as we only care about unique edges
neighbours = edges.groupByKey().map(lambda x : (x[0], list(set(list(x[1])))))

# want to produce: vertex1, (vertex2, [nb1,nb2,nb3...]):
edge_nb = edges.join(neighbours,10)

# remove edges where vertex1 has a degree that is too big for this algo
edge_nb = edge_nb.filter(degree_filter)

# now flip key and value: x[1][0] is our second vertex, x[1][1] is first neighbour list
edge_nb = edge_nb.map(lambda x: (x[1][0], (x[0], x[1][1])))

# perform join on the other neighbours
edge_nb = edge_nb.join(neighbours,20)

# now should have format, where nbij is neighbour j of vertex i:
# vertex2, ((vertex1, [nbv11, nbv12, nbv13, ...]), [nbv21, nbv22, nbv23, ...])

# find the size of list formed by intersection of neighbour lists
edge_intersections = edge_nb.map(lambda x: ((x[0], x[1][0][0]), len(list(set(x[1][0][1]) & set(x[1][1])))))

# separate vertices from the edge, keep the count for each
part1 = edge_intersections.map(lambda y: (y[0][0], y[1]))
part2 = edge_intersections.map(lambda y: (y[0][1], y[1]))

# concatenate these rdds
pre_counts = part1.union(part2)

# perform final summation of counts and divide by 3
summed_counts = pre_counts.reduceByKey(lambda a,b: a+b)
final = summed_counts.map(lambda x: (x[0], x[1]/3))

# produce 20 vertices with highest number of triangles
final20 = final.takeOrdered(20, key = lambda x: -x[1])
print(final20)
