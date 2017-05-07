from neo4j.v1 import GraphDatabase, basic_auth
from argparse import *
import progressbar
import  sys


DRIVER = None
SPLIT_NODE_INDEX = {}


def connect_to_db():
    global DRIVER

    if DRIVER is None:
        DRIVER = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))

    return DRIVER.session()


def open_files():
    try:
        # TODO: make data directory based on program arg
        # TODO: make 'velvetg_output' path a constant
        # TODO: make 'Graph' and 'PreGraph' filenames constats
        graph_file = open("velvetg_output/" + "yeast-fasta-output" + "/" + "Graph")

    except Exception as e:  # TODO: handle I/O Exception
        print(e)

    return graph_file


def delete_graph(session):
    print("Cleaning Graph Data...")

    #session.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

    print("\tDONE")
    print()


def create_node(session, node_id, seq_data_3):
    session.run("CREATE (node:Node {id: {node_id}, seq_data_3: {seq_data_3}})",
                {"node_id": node_id, "seq_data_3": seq_data_3})
    pass


def create_node_index(session):
    session.run("CREATE INDEX ON :Node(id)")


def count_nodes():
    print("Counting Nodes...")

    graph_file = open_files()

    node_count = 0

    line = graph_file.readline()
    while line != '':
        data = line.split()

        if len(data) != 0 and data[0] == "NODE":
            node_count += 1

        line = graph_file.readline()

    print("\t{0} Nodes detected".format(node_count))
    print()

    return node_count


def create_nodes(session):
    node_count = count_nodes()
    progress_bar = progressbar.ProgressBar(maxval=node_count).start()
    node_index = 0

    graph_file = open_files()
    graph_file.readline()

    print("Creating Nodes...")

    line = graph_file.readline()
    while line != '':
        data = line.split()

        if len(data) == 0 or data[0] != "NODE":
            break

        node_id = data[1]

        seq_data_3 = graph_file.readline().strip('\n')

        if len(seq_data_3) > 1:
            for i in range(1, len(seq_data_3) + 1):
                create_node(session, "{0}-{1}".format(node_id, i), seq_data_3[i - 1])

            SPLIT_NODE_INDEX[node_id] = ("{0}-{1}".format(node_id, 1),
                                         "{0}-{1}".format(node_id, len(seq_data_3)))

        else:
            create_node(session, node_id, seq_data_3)

        node_index += 1
        progress_bar.update(node_index)

        # TODO: create reverse-compliment node
        graph_file.readline()

        line = graph_file.readline()

    progress_bar.finish()
    print()

    create_node_index(session)

    return line


def create_relationship(session, src_id, dst_id):
    session.run("MATCH (src:Node {id: {src_id}}), (dst:Node {id: {dst_id}}) "
                "CREATE (src)-[:OVERLAPS]->(dst)",
                {"src_id": src_id, "dst_id": dst_id})


def count_type1_rels():
    print("Counting Type-1 Relationships...")

    rel1_count = 0

    for split_node_data in SPLIT_NODE_INDEX.values():
        split_node_range = int(split_node_data[1].split('-')[1])
        rel1_count += split_node_range

    print("\t{0} Type-1 Relationships detected".format(rel1_count))
    print()

    return rel1_count


def count_type2_rels():
    print("Counting Type-2 Relationships...")

    graph_file = open_files()

    rel2_count = 0

    line = graph_file.readline()
    while line != '':
        data = line.split()

        if len(data) != 0 and data[0] == "ARC" and int(data[1]) > 0 and int(data[2]) > 0:
            rel2_count += 1

        line = graph_file.readline()

    print("\t{0} Type-2 Relationships detected".format(rel2_count))
    print()

    return rel2_count


def create_relationships(session):
    print("Creating Relationships Part 1 / 2...")

    rel1_count = count_type1_rels()
    progress_bar1 = progressbar.ProgressBar(maxval=rel1_count).start()
    rel1_index = 0

    # create relationships for split nodes
    for node_id in SPLIT_NODE_INDEX.keys():
        split_node_data = SPLIT_NODE_INDEX[node_id]

        end_node = split_node_data[1]
        end_node_index = int(end_node.split('-')[1])

        for j in range(1, end_node_index + 1):
            create_relationship(session,
                                "{0}-{1}".format(node_id, j),
                                "{0}-{1}".format(node_id, j + 1))

            rel1_index += 1
            progress_bar1.update(rel1_index)

    progress_bar1.finish()
    print()

    print("Creating Relationships Part 2 / 2...")

    rel2_count = count_type2_rels()
    progress_bar2 = progressbar.ProgressBar(maxval=rel2_count).start()
    rel2_index = 0

    graph_file = open_files()

    # create relationships between LastGraph nodes
    line = graph_file.readline()
    while line != '':
        data = line.split()

        if len(data) == 0 or data[0] != "ARC":
            line = graph_file.readline()
            continue

        src_id = data[1]
        dst_id = data[2]

        if int(src_id) > 0 and int(dst_id) > 0:
            if src_id in SPLIT_NODE_INDEX:
                src_id = SPLIT_NODE_INDEX[src_id][1]

            if dst_id in SPLIT_NODE_INDEX:
                dst_id = SPLIT_NODE_INDEX[dst_id][0]

            create_relationship(session, src_id, dst_id)

            rel2_index += 1
            progress_bar2.update(rel2_index)

        line = graph_file.readline()

    progress_bar2.finish()
    print()


def create_graph():
    session = connect_to_db()

    #delete_graph(session)

    print("Creating Graph..")
    print()

    create_nodes(session)
    create_relationships(session)

    print("\tDone")
    print()


def enter_cli():
    pass


def main(flag_create):
    if flag_create:
        create_graph()

    enter_cli()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--create", action="store_true")

    args = parser.parse_args()

    main(args.create)


