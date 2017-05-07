from neo4j.v1 import GraphDatabase, basic_auth
from argparse import *
#import progressbar


DRIVER = None


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

    except Exception as e: # TODO: handle I/O Exception
        print(e)

    return graph_file


def delete_graph(session):
    session.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")


def create_nodes(session, graph_file):

    graph_file.readline()
    line = graph_file.readline()
    while line != '':
        if len(line) == 0:
            continue

        node_data = line.split()

        if node_data[0] != "NODE":
            break

        node_id = node_data[1]

        seq_data_3 = graph_file.readline().strip('\n')

        session.run("CREATE (node:Node {id: {node_id}, seq_data_3: {seq_data_3}})",
                    {"node_id": node_id, "seq_data_3": seq_data_3})

        # TODO: create reverse-compliment node
        # seq_data_5 = graph_file.readline()
        graph_file.readline()

        line = graph_file.readline()

    session.run("CREATE INDEX ON :Node(id)")

    return line


def create_relationships(session, graph_file, line):
    while line != '':
        arc_data = line.split()

        src_id = arc_data[1]
        dst_id = arc_data[2]

        if int(src_id) > 0 and int(dst_id) > 0:
            session.run("MATCH (src:Node {id: {src_id}}), (dst:Node {id: {dst_id}}) "
                        "CREATE (src)-[:OVERLAPS]->(dst)",
                        {"src_id": src_id, "dst_id": dst_id})

        line = graph_file.readline()


def create_graph():
    session = connect_to_db()

    graph_file = open_files()

    delete_graph(session)

    line = create_nodes(session, graph_file)
    create_relationships(session, graph_file, line)

    session.close()


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


