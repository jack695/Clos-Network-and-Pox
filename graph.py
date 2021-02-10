import copy
import warnings

class Topology:
    def __init__(self, cores):
        """
        Initializes the object.

        Parameters:
        -----------
        core: list of int
            List of the id of the core switches.
        """
        self.nodes = dict()
        self.cores_id = cores
        self.principal_core = None

    def add_node(self, id):
        """
        Add a node to the topology.

        Parameters:
        -----------
        id: int
            id of the switch.
        """
        if id not in self.nodes:
            self.nodes[id] = Node(id, True if id in self.cores_id else False)

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
        if id1 not in self.nodes:
            self.add_node(id1)
        if id2 not in self.nodes:
            self.add_node(id2)

        # Do nothing if the link already exists
        if port1 in self.nodes[id1].links and port2 in self.nodes[id2].links and self.nodes[id1].links[port1] == id2 and self.nodes[id2].links[port2] == id1:
            return

        # Warning displayed if the ports are already used
        if port1 in self.nodes[id1].links or port2 in self.nodes[id2].links:
            if port1 in self.nodes[id1].links:
                print("WARNING (add_link): port #{} of node #{} already used".format(port1, id1))
            if port2 in self.nodes[id2].links:
                print("WARNING (add_link): port #{} of node #{} already used".format(port2, id2))
            return

        self.nodes[id1].add_link(id2, port1)
        self.nodes[id2].add_link(id1, port2)

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
        if id1 not in self.nodes or port1 not in self.nodes[id1].links or id2 not in self.nodes or port2 not in self.nodes[id2].links:
            print("WARNING (remove_link): unknown link: node#{}:{} - node#{}:{}".format(id1, port1, id2, port2))
            return

        self.nodes[id1].remove_link(port1)
        self.nodes[id2].remove_link(port2)

    def port(self, src_id, dst_id):
        """
        Retrieves the port to go from one node to another

        Parameters:
        -----------
        src_id: int
            id of the source node
        dst_id:
            id of the destination node

        Return:
        -------
            The first port of the source node that lead to the destination. False if no link found.

        """
        for port, id in self.nodes[src_id].links.items():
            if id == dst_id:
                return port

        return None

    def spanning_tree(self):
        """
        Return a spanning tree using the reverse-delete algorithm. The links from the principal core node are removed
        in lastly.

        Return:
        -------
            The rooted tree and the mapping between the id of the switches and the blocked ports.
        """
        self._elect_principal_core()

        if self.principal_core is not None:
            cores_id = [k for k in self.nodes.keys() if k in self.cores_id and k != self.principal_core]
            cores_id.append(self.principal_core)
        else:
            cores_id = [k for k in self.nodes.keys() if k in self.cores_id]

        cores = [self.nodes.get(id) for id in cores_id]

        spanning_tree = copy.deepcopy(self)
        blocked_ports = {}
        for node_id in self.nodes.keys():
            # Returns the list of blocked ports for each host
            blocked_ports[node_id] = []

        # For every link we check if we can remove it from the graph and keeping the connectivity
        # It is a bipartite, checking the links from the core nodes is enough
        for core in cores:
            for port1, id2 in core.links.items():
                id1 = core.id
                for port, id in self.nodes[id2].links.items():
                    if id == id1:
                        port2 = port

                # Remove a link and check if the tree is still connected. If not -> add the link again
                spanning_tree.remove_link(id1, id2, port1, port2)
                if spanning_tree._is_connected() == False:
                    spanning_tree.add_link(id1, id2, port1, port2)
                else:
                    blocked_ports[id1].append(port1)
                    blocked_ports[id2].append(port2)

        return spanning_tree, blocked_ports

    def rooted_tree(self, root):
        """
        Similar to the spanning tree, but only let the links of the rooted node active. The connectivity between edge
        and non rooted core switches is not handled. Returns also a dictionary of blocked ports for each node.

        Parameters:
        -----------
            root: int
                The id of the core switch to use to connect to the edge switches.

        Return:
        -------
            The rooted tree and the mapping between the id of the switches and the blocked ports.
        """
        # Check that root is the id of a core switch
        if root not in self.cores_id:
            raise ValueError("{} is not the id of a root switch".format(root))

        # Check that the root is connected to all edge switches
        edges_id = [k for k in self.nodes.keys() if k not in self.cores_id]
        neighbors = list(self.nodes[root].links.values())
        if neighbors != edges_id:
            raise ValueError("Root core switch selected and not fully connected to the edge switch")

        # Select the other cores switch
        cores_id = [k for k in self.nodes.keys() if k in self.cores_id and k != root]
        cores = [self.nodes.get(id) for id in cores_id]

        rooted_tree = copy.deepcopy(self)
        blocked_ports = {}
        for node_id in self.nodes.keys():
            # Returns the list of blocked ports for each host
            blocked_ports[node_id] = []

        # Remove their link
        for core in cores:
            for port1, id2 in core.links.items():
                id1 = core.id
                for port, id in self.nodes[id2].links.items():
                    if id == id1:
                        port2 = port

                rooted_tree.remove_link(id1, id2, port1, port2)
                blocked_ports[id1].append(port1)
                blocked_ports[id2].append(port2)

        return rooted_tree, blocked_ports

    def fully_connected_core(self):
        """
        Return:
        -------
             The list of the core switches that are connected to every edge switches
        """
        core_nodes = [v for k, v in self.nodes.items() if k in self.cores_id]
        edges_id = [k for k in self.nodes.keys() if k not in self.cores_id]

        fully_conn_core = []

        for node in core_nodes:
            neighbors = list(node.links.values())
            # Elect the first core node that is fully connected to the edge nodes
            if neighbors == edges_id:
                fully_conn_core.append(node.id)

        return fully_conn_core

    def _is_connected(self):
        """
        Check if the topology is connected.

        Return:
        -------
            True if the topology is connecter, False otherwise
        """
        unvisited = list(self.nodes.keys())
        node = unvisited[0]
        frontier = [node]
        visited = []

        while frontier:
            node = frontier.pop()
            visited.append(node)
            # Remove this node from the list of unvisited nodes
            if node in unvisited:
                unvisited.remove(node)
            for id in self.nodes[node].links.values():
                if id not in frontier and id not in visited:
                    frontier.append(id)

        return True if not unvisited else False

    def _elect_principal_core(self):
        """
        Elect a new principal core switch

        Returns:
        --------
            The id of the core switch elected, None if no switch was elected.
        """
        core_nodes = [v for k, v in self.nodes.items() if k in self.cores_id]

        edges_id = [k for k in self.nodes.keys() if k not in self.cores_id]

        for node in core_nodes:
            neighbors = list(node.links.values())
            # Elect the first core node that is fully connected to the edge nodes
            if neighbors == edges_id:
                self.principal_core = node.id
                return
        self.principal_core = None

    def debug(self):
        for node in self.nodes.values():
            node.debug()


class Node:
    def __init__(self, id, core):
        """
        Initializes the object

        Parameters:
        -----------
        id: int
            id of the switch
        core: boolean
            Determine if this node is a core switch or not.

        """
        self.id = id
        self.links = dict()
        self.core = core

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
        self.links[port] = id

    def remove_link(self, port):
        """
        Remove a link from the node

        Parameters:
        -----------
        port: int
            port that leads to the neighbor

        """
        del self.links[port]

    def debug(self):
        s = ''
        for key, value in self.links.items():
            s += str(key) + ':' + str(value) + ' '

        print('node_#{}: {}- core={}'.format(self.id, s, self.core))


if __name__ == '__main__':
    t = Topology([1, 2, 3])
    t.add_link(1, 4, 1, 1)
    t.add_link(1, 5, 2, 1)
    t.add_link(1, 6, 3, 1)
    t.add_link(1, 7, 4, 1)
    t.add_link(2, 4, 1, 2)
    t.add_link(2, 5, 2, 2)
    t.add_link(2, 6, 3, 2)
    t.add_link(2, 7, 4, 2)
    t.add_link(3, 4, 1, 3)
    t.add_link(3, 5, 2, 3)
    t.add_link(3, 6, 3, 3)
    t.add_link(3, 7, 4, 3)
    """
    st = t.spanning_tree()
    t.debug()
    st.debug()"""
    """
    t.remove_link(1, 4, 1, 1)
    t.remove_link(2, 4, 1, 2)
    t.remove_link(3, 5, 2, 3)
    st = t.spanning_tree()
    t.debug()
    st.debug()"""
    st, ports = t.rooted_tree(1)
    st.debug()
    print(ports)

    print(t.port(4, 3))

    pass