from neo4j.v1 import GraphDatabase, basic_auth


DRIVER = None

def reverse_compliment(seq):
    result = ""

    for c in seq:
        if c == 'A':
            result = 'T' + result
        elif c == 'T':
            result = 'A' + result
        elif c == 'G':
            result = 'C' + result
        elif c == 'C':
            result = 'G' + result
        elif c == 'N':
            result = 'N' + result
        else:
            raise Exception

    return result


def count_nodes():
    f = open("velvetg_output/yeast-fasta-output/yeast.fasta")

    total_count = 0
    seq_count = 0

    line = f.readline()
    while line != '':
        if len(line) > 0 and line[0] != '>':
            if line[-1] == '\n':
                seq_count += len(line) - 1
            else:
                seq_count += len(line)
        else:
            total_count += seq_count - 15 + 1
            seq_count = 0

        line = f.readline()

    total_count += seq_count - 15 + 1

    print(total_count)


def connect_to_db():
    global DRIVER

    DRIVER = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
    return DRIVER.session()


def open_files():
    try:
        # TODO: make data directory based on program arg
        # TODO: make 'velvetg_output' path a constant
        # TODO: make 'Graph' and 'PreGraph' filenames constats
        graph_file = open("velvetg_output/" + "yeast-fasta-output" + "/" + "Graph")

    except Exception as e: # TODO: handle I/O Exception
        print(e)

    return graph_file


def create_nodes(session, graph_file):
    session.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

    node_table = {}

    graph_file.readline()
    line = graph_file.readline()
    while line != '':
        node_data = line.split()

        if node_data[0] != "NODE":
            break

        node_id = node_data[1]

        seq_data_3 = graph_file.readline().strip('\n')
        node_table[node_id] = (node_id, seq_data_3)

        session.run("CREATE (node:Node {id: {node_id}, seq_data_3: {seq_data_3}})",
                    {"node_id": node_id, "seq_data_3": seq_data_3})

        # TODO: create reverse-compliment entry
        # seq_data_5 = graph_file.readline()
        graph_file.readline()

        # TODO: create reverse-compliment node

        line = graph_file.readline()

    while line != '':
        arc_data = line.split()

        src_id = arc_data[1]
        dst_id = arc_data[2]

        if int(src_id) > 0 and int(dst_id) > 0:
            session.run("MATCH (src:Node {id: {src_id}}), (dst:Node {id: {dst_id}}) CREATE (src)-[:OVERLAPS]->(dst)",
                        {"src_id": src_id, "dst_id": dst_id})

        line = graph_file.readline()


def build_graph(graph_file):
    session = connect_to_db()

    create_nodes(session, graph_file)

    session.close()


def main():
    graph_file = open_files()

    build_graph(graph_file)


if __name__ == "__main__":
    main()

