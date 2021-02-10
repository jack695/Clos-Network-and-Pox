from pox.core import core
import pox.openflow.discovery
import pox.openflow.libopenflow_01 as of
from misc.generic import CentralController, SwitchController
from misc.graph import *
import tenants

log = core.getLogger()


class VLANSwitchController(SwitchController):
    """
    A VLANSwitchController instance handles the behavior of the controller for a
    given switch. It forwards packet when it knows the mapping between the destination address and the port. Otherwise,
    according to the VLAN belonging of the packet, if floods on
    the non-blocking ports for this VLAN.
    """
    def __init__(self, connection):
        """
        Initializes the switch controller.

        Parameters:
        -----------
        connection: Connection
                    A connection objet to the switch.
        """
        super(VLANSwitchController, self).__init__(connection)

        self.mac_to_port = {}
        self.vlan_to_core = None
        self.core_to_ports = None

    def block_ports_vlan(self, vlan_to_core, core_to_ports):
        """
        Blocks some ports for some VLAN and reset the learned destination-port mapping.

        Parameters:
        -----------
        vlan_to_core: dict
            The mapping between the different VLANs and the id of the core switch to use.
        core_to_ports: dict
            The mapping between the id of the core switch and the blocked ports of the rooted tree associated to this switch.

        """
        self.vlan_to_core = vlan_to_core
        self.core_to_ports = core_to_ports
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
        # log.debug("On packet in %d: %s, %s" % (self.connection.dpid, event.parsed, event.ofp))
        packet = event.parsed
        packet_in = event.ofp

        # Update the mac to port binding
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
            # Determine the vlan whose belongs the packet
            if packet.src in tenants.hosts:
                vlan = tenants.hosts[packet.src]
            else:
                vlan = 'default'

            forwarding_core = self.vlan_to_core[vlan]
            blocked_ports = (self.core_to_ports[forwarding_core])[self.connection.dpid]

            # Tell the switch to broadcast the packet according to the right vlan tree
            for port in [p for p in self.ports if (p != packet_in.in_port and p not in blocked_ports)]:
                self._send_packet_out(packet_in, port)
            log.debug("switch #{} flood on ports [{}]".format(self.connection.dpid, [p for p in self.ports if (p != packet_in.in_port and p not in blocked_ports)]))


class VLANController(CentralController):
    """
    A VLANController that initializes and keeps track of one VLANSwitchController per switch connection.
    """

    def __init__(self, core_ids):
        """
        Initializes the main controller.

        Parameters:
        -----------
        core_ids: list
            List of the ids of the core switches.

        """
        super(VLANController, self).__init__(core_ids)

    def _handle_ConnectionUp(self, event):
        """
        Handle new switch connections.

        Parameters:
        -----------
        event: Event
            Event that triggered this function.

        """
        self.switch_controllers.append(VLANSwitchController(event.connection))

    def _handle_LinkEvent(self, event):
        """
        Handles links going up or down. The spanning tree is recomputed each time.

        Parameters:
        -----------
        event: Event
            Event that triggered this function.

        """
        super(VLANController, self)._handle_LinkEvent(event)

        # Check the information provided by the user in tenants.py
        vlans = list(set(tenants.hosts.values()))
        if len(vlans) != tenants.vlan_count:
            log.debug("The number of vlans defined by the user does not match with 'vlan_count' in tenants.py")

        # Get the list of the core switches that a are fully connected to the edge switches
        principal_cores = self.topology.fully_connected_core()

        # This algorithm behaves well only if there is at least one core switch fully connected
        if principal_cores:
            # Create a mapping between each VLAN and the core in charge of it
            vlan_to_core = {}
            t = 0
            for vlan in vlans:
                vlan_to_core[vlan] = principal_cores[t]
                t = (t+1) % len(principal_cores)
            vlan_to_core['default'] = principal_cores[t]

            # Get the blocked ports for every VLAN tree
            core_to_ports = {}
            for core in principal_cores:
                t, p = self.topology.rooted_tree(core)
                core_to_ports[core] = p

            # Forward this information to the switch controllers
            for switch_controller in self.switch_controllers:
                switch_controller.block_ports_vlan(vlan_to_core, core_to_ports)


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
    controller = VLANController(core_ids)
    core.register(controller)
