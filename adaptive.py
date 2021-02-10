from pox.core import core
import pox.openflow.discovery
import pox.openflow.libopenflow_01 as of
from pox.lib.recoco import Timer
from pox.lib.packet.ethernet import ETHER_BROADCAST, ETHER_ANY
import pox.host_tracker
import random


# TODO Share known redirections between core switches.
# NOTE We assume that port X on any edge switch goes to core switch X
# NOTE For PortStatsReceived, we assume that event.stats[0] is for port 65534
# NOTE For ConnectionUp, we assume that event.connection.ports[-1] is port 65534

log = core.getLogger()

class AdaptiveSwitchController():
    def __init__(self, connection):
        self.connection = connection
        self.dpid = connection.dpid
        self.timer = None

        # Add listeners
        self.connection.addListeners(self)

    def _handle_PacketIn(self, event):
        raise NotImplemented()

    def _send_packet_out(self, of_packet, out_port):
        """
        Send the packet to the specified port.
        """
        msg = of.ofp_packet_out()
        msg.data = of_packet
        action = of.ofp_action_output(port=out_port)
        msg.actions.append(action)
        self.connection.send(msg)

    def _forward_and_update(self, raw_packet, of_packet, out_port):
        """
        Send the packet on the specified port and define a flow rule for the following
        packets having the same source, destination and type.
        """
        # Forward packet
        self._send_packet_out(of_packet, out_port)

        # Update switch flows
        msg = of.ofp_flow_mod()
        match = of.ofp_match()
        match.dl_src = raw_packet.src
        match.dl_dst = raw_packet.dst
        match.type = raw_packet.type
        msg.match = match
        msg.actions.append(of.ofp_action_output(port = out_port))
        self.connection.send(msg)


class AdaptiveCoreSwitchController(AdaptiveSwitchController):
    def __init__(self, connection, interval):
        AdaptiveSwitchController.__init__(self, connection)
        
        self.interval = interval
        self.mac_to_port = {}

        # Start to request stats if core switch
        self._request_port_stats()

    def _request_port_stats(self):
        """
        Ask to a core switch for statistics feedback. The response is handled by the
        main controller instance.
        """
        # Request
        self.connection.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))

        # Schedule next execution
        self.timer = Timer(self.interval, self._request_port_stats)

    def _handle_PacketIn(self, event):
        """
        Callback invoked when a packet out request has been received.
        """
        # Update MAC to port mapping
        raw_packet = event.parsed
        of_packet = event.ofp
        self.mac_to_port[raw_packet.src] = of_packet.in_port
        
        # Define the behavior of the switch
        if raw_packet.dst in self.mac_to_port:
            out_port = self.mac_to_port[raw_packet.dst]
            self._forward_and_update(raw_packet, of_packet, out_port)
        else:
            self._send_packet_out(of_packet, of.OFPP_ALL)


class AdaptiveEdgeSwitchController(AdaptiveSwitchController):
    def __init__(self, connection, core_ports, links, hosts, ports):
        AdaptiveSwitchController.__init__(self, connection)

        self.core_ports = core_ports
        self.links = links
        self.hosts = hosts
        self.ports = ports

    def _host_broadcast_packet_out(self, of_packet):
        """
        Send the packet to all hosts directly connected to the edge switch.
        """
        out_ports = set(self.connection.ports.keys()[:-1]) - set(self.core_ports)
        for port in out_ports:
            self._send_packet_out(of_packet, port)

    def _handle_PacketIn(self, event):
        """
        Callback invoked when a packet out request has been received.
        """
        raw_packet = event.parsed
        of_packet = event.ofp
        links = {t[1]: self.links[t] for t in self.links if t[0] == self.dpid}
        hosts = self.hosts[self.dpid] if self.dpid in self.hosts else set()
        ports = self.ports[self.dpid] if self.dpid in self.ports else dict()

        # If the destination is a direct host
        if raw_packet.dst in hosts and \
            raw_packet.dst in ports:
            # Send to host and create flow
            self._forward_and_update(raw_packet, of_packet, ports[raw_packet.dst])
        else:
            # Pick a core to handle the new flow
            if links:
                out_port = min(links, key=links.get)
            else:
                out_port = random.choice(self.core_ports)
            
            # If the destination is a foreign host
            if raw_packet.dst != ETHER_BROADCAST and \
                raw_packet.dst != ETHER_ANY and \
                raw_packet.dst not in hosts and \
                (len(hosts) + len(self.core_ports) == len(self.connection.ports) - 1):
                # Send to THE ONE and create flow
                self._forward_and_update(raw_packet, of_packet, out_port)
            # If the destination is unknown and previous hop is core
            elif of_packet.in_port in self.core_ports:
                # Broadcast locally
                self._host_broadcast_packet_out(of_packet)
            # If the destination is unknown and previous hop is host 
            else:
                # Broadcast locally and send to THE ONE
                self._send_packet_out(of_packet, out_port)
                self._host_broadcast_packet_out(of_packet)


class AdaptiveController():
    def __init__(self, core_ids, interval):
        self.core_ids = core_ids
        self.interval = interval
        self.switch_controllers = {}
        self.core_to_edge = {}
        self.edge_links = {}
        self.prev_edge_links = {}
        self.edge_hosts = {}
        self.edge_host_ports = {}

        # Add listeners
        core.openflow.addListenerByName("ConnectionUp", self._handle_ConnectionUp)
        core.openflow_discovery.addListenerByName("LinkEvent", self._handle_LinkEvent)
        core.openflow.addListenerByName("PortStatsReceived", self._handle_PortStatsReceived)
        core.host_tracker.addListenerByName("HostEvent", self._handle_HostEvent)

    def _handle_ConnectionUp(self, event):
        """
        Callback invoked when a new switch connects to the network.
        """
        # Create the coresponding switch controller instance to handle the new connection
        dpid = event.connection.dpid
        if dpid in self.core_ids:
            switch_controller = AdaptiveCoreSwitchController(event.connection, self.interval)
        else:
            switch_controller = AdaptiveEdgeSwitchController(event.connection, self.core_ids, \
            self.edge_links, self.edge_hosts, self.edge_host_ports)
        self.switch_controllers[dpid] = switch_controller

    def _handle_LinkEvent(self, event):
        """
        Callback invoked when a link has been removed or added to the network.
        """
        # Split the link into core and edge parts
        raw = event.link.uni
        if raw.dpid1 in self.core_ids:
            core, edge = (raw.dpid1, raw.port1), (raw.dpid2, raw.port2)
        else:
            core, edge = (raw.dpid2, raw.port2), (raw.dpid1, raw.port1)

        # Update the maps
        if event.added:
            self.core_to_edge[core] = edge
            self.edge_links[edge] = 0
            self.prev_edge_links[edge] = 0
        elif event.removed:
            self.core_to_edge[core] = None
            self.edge_links[edge] = None
            self.prev_edge_links[edge] = None

    def _handle_PortStatsReceived(self, event):
        """
        Callback invoked when a switch statistics response has arrived to the controller.
        """
        # Update link stats for all relevant ports (i.e. not the 65534 at index 0)
        dpid = event.connection.dpid
        for s in event.stats[1:]:
            core = (dpid, s.port_no)
            if core in self.core_to_edge:
                edge = self.core_to_edge[core]
                if edge in self.edge_links and edge in self.prev_edge_links:
                    total = s.tx_bytes + s.rx_bytes
                    self.edge_links[edge] = total - self.prev_edge_links[edge]
                    self.prev_edge_links[edge] = total

    def _handle_HostEvent(self, event):
        """
        Callback invoked when a new host has been discovered in the network.
        """
        # Update host tracking
        dpid = event.entry.dpid
        if dpid not in self.edge_hosts:
            self.edge_hosts[dpid] = set()
        self.edge_hosts[dpid].add(event.entry.macaddr)

        # Update host port
        if dpid not in self.edge_host_ports:
            self.edge_host_ports[dpid] = {}
        self.edge_host_ports[dpid][event.entry.macaddr] = event.entry.port


def launch(core_ids, interval):
    """
    Launch the adaptive routing component.
    """
    # Launch all additional components
    pox.openflow.discovery.launch()
    pox.host_tracker.launch()

    # Check arguments
    try:
        core_ids = list(map(int, core_ids.split(",")))
        interval = float(interval)
    except ValueError:
        raise ValueError('This controller requires the list of core ids separated by a comma. \
        (e.g. --core_ids=1,2)')

    print("Arguments: core_ids={} interval={}".format(core_ids, interval))
    
    # Register controller
    controller = AdaptiveController(core_ids, interval)
    core.register(controller)
