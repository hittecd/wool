from neo4j.v1 import GraphDatabase, basic_auth


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


def connect_to_db():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
    return driver.session()


def open_files():
    pass


def build_graph():
    '''
    session.run("CREATE (a:Person {name: {name}, title: {title}})",
                {"name": "Arthur", "title": "King"})

    result = session.run("MATCH (a:Person) WHERE a.name = {name} "
                         "RETURN a.name AS name, a.title AS title",
                         {"name": "Arthur"})

    for record in result:
        print("%s %s" % (record["title"], record["name"]))
    '''
    pass


def main():
    session = connect_to_db()

    open_files()

    build_graph()

    session.close()


if __name__ == "__main__":
    main()

