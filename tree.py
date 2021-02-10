from pox.core import core
import pox.openflow.discovery
import pox.openflow.libopenflow_01 as of
from misc.generic import CentralController, SwitchController
from misc.graph import *

log = core.getLogger()


class TreeSwitchController(SwitchController):
    """
    A TreeSwitchController instance handles the behavior of the controller for a
    given switch. It forwards packet when it knows the mapping between the destination address and the port, flood on
    the non-blocking ports otherwise.
    """

    def __init__(self, connection):
        """
        Initializes the switch controller.

        Parameters:
        -----------
        connection: Connection
                    A connection objet to the switch.
        """
        super(TreeSwitchController, self).__init__(connection)

        self.mac_to_port = {}
        self.blocked_ports = []

    def block_ports(self, ports):
        """
        Blocks the ports given by the user and reset the learned destination-port mapping.

        Parameters:
        -----------
        ports: list
            List of the switch ports to block.

        """
        log.debug('Switch #{} - Blocked ports: {}'.format(self.connection.dpid, ports))
        self.blocked_ports = ports
        # Reset the mapping to let the switch to adapt learn the new topology
        self.mac_to_port = {}

    def _handle_PacketIn(self, event):
        """
        Handles packet in messages from the switch.

        Parameters:
        -----------
        event: Event
                Event that triggers the PacketIn callback function.

        """
        packet = event.parsed
        packet_in = event.ofp

        # Update the mac to port binding only if the packet is coming from a non blocking port
        if packet_in not in self.blocked_ports:
            self.mac_to_port[packet.src] = packet_in.in_port

        # If we know how to reach the destination
        if packet.dst in self.mac_to_port:
            out_port = self.mac_to_port[packet.dst]

            # Forward the packet
            self._send_packet_out(packet_in, out_port)

            # Install a flow on the switch
            self._flow_mod_msg(packet.src, packet.dst, out_port, hard_timeout=10)
        # If we do not know the destination
        else:
            # Tell the switch to broadcast the packet except on incoming port, blocking ports
            for port in [p for p in self.ports if (p != packet_in.in_port and p not in self.blocked_ports)]:
                self._send_packet_out(packet_in, port)
            log.debug("switch #{} flood on ports [{}]".format(self.connection.dpid, [p for p in self.ports if (p != packet_in.in_port and p not in self.blocked_ports)]))


class TreeController(CentralController):
    """
    A TreController that initializes and keeps track of one TreeSwitchController per switch connection.
    """

    def __init__(self, core_ids):
        """
        Initializes the main controller.

        Parameters:
        -----------
        core_ids: list
            List of the ids of the core switches.

        """
        super(TreeController, self).__init__(core_ids)

    def _handle_ConnectionUp(self, event):
        """
        Handle new switch connections.

        Parameters:
        -----------
        event: Event
            Event that triggered this function.

        """
        self.switch_controllers.append(TreeSwitchController(event.connection))

    def _handle_LinkEvent(self, event):
        """
        Handles links going up or down. The spanning tree is recomputed each time.

        Parameters:
        -----------
        event: Event
            Event that triggered this function.

        """
        super(TreeController, self)._handle_LinkEvent(event)

        self.spanning_tree, blocked_ports = self.topology.spanning_tree()

        log.debug(blocked_ports)
        for switch in self.switch_controllers:
            # "If" required because we could have established the connection to a switch but no links active right now
            if switch.connection.dpid in blocked_ports:
                switch.block_ports(blocked_ports[switch.connection.dpid])


def launch(core_ids):
    """
    Starts the controller component.
    """
    # Launch all additional components
    pox.openflow.discovery.launch()

    # Register the controller
    try:
        core_ids = list(map(int, core_ids.split(",")))
    except ValueError:
        raise ValueError('This controller requires the list of core ids separated by a comma. (e.g. --core_ids=1,2)')
    controller = TreeController(core_ids)
    core.register(controller)
