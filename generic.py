from pox.core import core
import pox.openflow.discovery
import pox.openflow.libopenflow_01 as of
from misc.graph import *

log = core.getLogger()


class SwitchController(object):
    """
    A SwitchController instance handles the behavior of the controller for a
    given switch.
    """

    def __init__(self, connection):
        """
        Initializes the switch controller.

        Parameters:
        -----------
        connection: Connection
                    A connection objet to the switch.
        """
        connection.addListeners(self)
        self.connection = connection

        # Get the list of ports that the switch owns
        self.ports = []
        for port in connection.features.ports:
            # Do not use the port 65534
            if port.port_no != 65534:
                self.ports.append(port.port_no)

    def _handle_PacketIn(self, event):
        """
        Handles packet in messages from the switch.
        """
        raise NotImplementedError()

    def _send_packet_out(self, packet_in, out_port):
        """
        Instructs the switch to resend a packet that it had sent to us.
        """
        # Build PACKET_OUT message
        msg = of.ofp_packet_out()
        msg.data = packet_in
        action = of.ofp_action_output(port=out_port)
        msg.actions.append(action)

        # Send message to switch
        self.connection.send(msg)

    def _flow_mod_msg(self, src, dst, out_port, hard_timeout=30):
        """
        Install a new rule for a flow.

        Parameters:
        -----------
        src: EthAddr
             MAC address of the source
        dst: EthAddr
             MAC address of the destination
        out_port: int
            Port to which the packet must be forward.
        hard_timeout: int
            delay after what, the flow is dropped from the flows table.

        """
        # Build FLOW_MOD message
        msg = of.ofp_flow_mod()
        match = of.ofp_match()
        match.dl_dst = dst
        match.dl_src = src
        msg.match = match
        msg.actions.append(of.ofp_action_output(port=out_port))
        # Add hard time out
        msg.hard_timeout = hard_timeout

        # Send message to switch
        self.connection.send(msg)

        log.debug("Add flow entry in switch #{}: {} {} {}".format(self.connection.dpid, src, out_port, dst))


class CentralController(object):
    def __init__(self, core_ids):
        """
        Initializes the main controller.

        Parameters:
        -----------
        core_ids: list
            List of the ids of the core switches.

        """
        self.core_ids = core_ids
        self.topology = Topology(core_ids)
        self.switch_controllers = []

        # Add the listeners
        core.openflow.addListenerByName("ConnectionUp", self._handle_ConnectionUp)
        core.openflow_discovery.addListenerByName("LinkEvent", self._handle_LinkEvent)

    def _handle_ConnectionUp(self, event):
        """
        Handle new switch connections.
        """
        raise NotImplementedError()

    def _handle_LinkEvent(self, event):
        """
        Handles links going up or down. The topology is changed according to the event.

        Parameters:
        -----------
        event: Event
            Event that triggered this function.

        """
        if event.added:
            self.topology.add_link(event.link.dpid1, event.link.dpid2, event.link.port1, event.link.port2)
        elif event.removed:
            self.topology.remove_link(event.link.dpid1, event.link.dpid2, event.link.port1, event.link.port2)
