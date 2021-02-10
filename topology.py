import copy

class Node(object):
    """
    Class representing a node in the topology. (In our case, a switch)
    """
    def __init__(self, id):
        """
        Initializes a node object.

        Parameters:
        -----------
        id: int
            id of the object.
        """
        self.id = id
        self._links = dict()

    def add_link(self, id, port):
        """
        Add a link to the node.

        Parameters:
        -----------
        id: int
            Id of the neighbor node
        port: int
            port that leads to the neighbor
        """
        self._links[port] = id

    def remove_link(self, port):
        """
        Remove a link from the node

        Parameters:
        -----------
        port: int
            port that leads to the neighbor

        """
        del self._links[port]

    def neighbors(self):
        """
        Returns the list of neighbors of this node

        Return:
        -------
        The list of neighbors of this node

        """
        return list(self._links.values())


class Topology(object):
    def __init__(self):
        """
        Initializes the object.

        """
        self._nodes = dict()
        self._links = list() # Field used for the spanning tree algorithm

    def add_link(self, id1, id2, port1, port2):
        """
        Adds a link between two nodes. If the nodes do not exist, they are created.

        Parameters:
        -----------
        id1: int
            Id of the first node
        id2: int
            Id of the second node
        port1: int
            Port of the first node leading to the second one
        port2: int
            Port of the second node leading to the first one.

        """
        if id1 not in self._nodes:
            self._nodes[id1] = Node(id1)
        if id2 not in self._nodes:
            self._nodes[id2] = Node(id2)

        self._nodes[id1].add_link(id2, port1)
        self._nodes[id2].add_link(id1, port2)

        if id1 <= id2 and tuple([id1, id2, port1, port2]) not in self._links:
            self._links.append(tuple([id1, id2, port1, port2]))
        elif id1 > id2 and tuple([id2, id1, port2, port1]) not in self._links:
            self._links.append(tuple([id2, id1, port2, port1]))

    def remove_link(self, id1, id2, port1, port2):
        """
        Removes a link between two nodes.

        Parameters:
        -----------
        id1: int
            Id of the first node
        id2: int
            Id of the second node
        port1: int
            Port of the first node leading to the second one
        port2: int
            Port of the second node leading to the first one.

        """
        self._nodes[id1].remove_link(port1)
        self._nodes[id2].remove_link(port2)

        if id1 <= id2:
            self._links.remove(tuple([id1, id2, port1, port2]))
        else:
            self._links.remove(tuple([id2, id1, port2, port1]))

    def is_connected(self):
        """
        Check if the topology is connected.

        Returns:
        --------
        True if the topology is connected, False otherwise.

        """
        starting_node = list(self._nodes.values())[0].id # Take the first element to start the algorithm
        frontier = [starting_node] # Contains the nodes to expand
        unvisited_nodes = list(self._nodes.keys())
        visited_nodes = []

        while frontier:
            # Expand a new node
            node = frontier.pop(0)
            visited_nodes.append(node)
            unvisited_nodes.remove(node)

            for neigbhor in self._nodes[node].neighbors():
                if neigbhor not in visited_nodes and neigbhor not in frontier:
                    frontier.append(neigbhor)

        # Check if all the nodes have been visited
        return bool(not unvisited_nodes)

    def spanning_tree(self):
        """
        Computes a spanning tree of the graph.

        Returns:
        --------
        A spanning tree of the topology.

        """
        new_graph = copy.deepcopy(self)

        for link in new_graph._links:
            new_graph.remove_link(link[0], link[1], link[2], link[3])
            if not new_graph.is_connected():
                new_graph.add_link(link[0], link[1], link[2], link[3])

        return new_graph

    def to_str(self):
        return str(self._links)
