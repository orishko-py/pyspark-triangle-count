
import pyspark
import itertools

# SparkContext.setSystemProperty('spark.executor.memory', '10g')
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


def generate_combinations(line):
    # create all combinations and return key-value pairs in format ((v1,v2), ['to_check'])
    l = line[1]
    return [(pair, ['to_check']) for pair in itertools.combinations(l, 2)]


def degree_filter(t):
    # not considering vertices with degree less than 2
    # as there are no possible wedges/closed wedges which they could form
    if len(t[1]) < 2:
        return False
    return True


lines = sc.textFile("/data/ethereum/transactionSmall/part-00019")
clean_lines = lines.filter(is_good_line)

# will check for edges only, so create key-value pairs ((v1, v2), 'exists')
existing_edges_1 = clean_lines.map(lambda l: ((l.split(',')[1], l.split(',')[2]), ['exists']))
existing_edges_2 = clean_lines.map(lambda l: ((l.split(',')[2], l.split(',')[1]), ['exists']))
existing_edges = existing_edges_1.union(existing_edges_2)

# tests
print(existing_edges.count())
print(existing_edges.takeSample(False, 5))

edges_1 = clean_lines.map(lambda l: (l.split(',')[1], l.split(',')[2]))
edges_2 = clean_lines.map(lambda l: (l.split(',')[1], l.split(',')[2]))

edges = edges_1.union(edges_2)

# create rdd that has key-value pairs
# (vertex1, [nb1,nb2,nb3...]), where nbi is neighbour i of vertex1
neighbours = edges.groupByKey().map(lambda x : (x[0], list(set(list(x[1])))))
neighbours = neighbours.filter(degree_filter)

# generate all pairs ((v1, v2), ['to_check'])
# use flatMap as generate_combinations returns lists
edges_to_check = neighbours.map(generate_combinations).flatMap(lambda x: x)

# tests
print(edges_to_check.count())
print(edges_to_check.takeSample(False, 5))

# perform summation over lists containing 'to_check'
edges_to_check = edges_to_check.reduceByKey(lambda a,b: a+b)

# create rdd in the format ((v1, v2), ('exists', ['to_check', 'to_check',...]))
all_edges = existing_edges.join(edges_to_check)

# tests
print(all_edges.count())
print(all_edges.takeSample(False, 5))

# finding count
all_edges = all_edges.map(lambda x: (x[0], len(x[1][1])))

# separate count for each vertex
vertices_part1 = all_edges.map(lambda y: (y[0][0], y[1]))
vertices_part2 = all_edges.map(lambda y: (y[0][1], y[1]))
vertex_pair = vertices_part1.union(vertices_part2)

print(vertex_pair.takeSample(False, 5))

# summing over all vertex counts
vertex = vertex_pair.reduceByKey(lambda a,b: a+b)
final = vertex.map(lambda x: (x[0], x[1]/6))

# produce 20 vertices with highest number of triangles
final20 = final.takeOrdered(20, key = lambda x: -x[1])
print(final20)
