"""
Flow Log Fields:
                        start_time	string	When the first byte in a flow log was captured and seen in the data path (RFC 3339 Date and Time - Coordinated Universal Time).
                        end_time	string	When the last byte in a flow log was captured and seen in the data path (RFC 3339 Date and Time - Coordinated Universal Time).
            connection_start_time	string	When the first byte in a flow log’s connection was captured and seen in the data path (RFC 3339 Date and Time - Coordinated Universal Time).
                        direction	string	Values are I for inbound or O for outbound. If the first packet on the connection was received by the vNIC, the direction is I. If the first packet was sent by the vNIC, the direction is O.
                            action	string	Values are accepted (traffic summarized by this flow was accepted) or rejected (traffic was rejected).
                    initiator_ip	string	(IPv4 address) Source-IP as it appears in the first packet that is processed by the vNIC on this connection. If direction=="outbound", a private IP associated with the vNIC.
                        target_ip	string	(IPv4 address) Dest-IP as it appears in the first packet that is processed by the vNIC on this connection. If direction=="inbound", a private IP associated with the vNIC.
                    initiator_port	uint16	The TCP/UDP source-port as it appears in first packet that is processed by this vNIC on this connection.
                        target_port	uint16	The TCP/UDP dest-port as it appears in first packet that is processed by this vNIC on this connection.
                transport_protocol	uint8	The Internet Assigned Numbers Authority (IANA) protocol number (TCP or UDP). (6: tcp, 17: udp)
                        ether_type	string	Currently, IPv4 is the only value.
                    was_initiated	bool	The connection initiated in this flow log.
                    was_terminated	bool	The connection terminated (for example, timeout/RST/Final-FIN).
            bytes_from_initiator	uint64	The count of bytes on connection in this flow log’s time-window, from Initiator to Target.
            packets_from_initiator	uint64	The count of packets on the connection in this flow log’s time-window, from Initiator to Target.
                bytes_from_target	uint64	The count of bytes on the connection in this flow log’s time-window, from Target to Initiator.
                packets_from_target	uint64	The count of packets on the connection in this flow log’s time-window, from Target to Initiator.
    cumulative_bytes_from_initiator	uint64	The count of bytes since the connection was initiated, from Initiator to Target.
cumulative_packets_from_initiator	uint64	The count of packets since the connection was initiated, from Initiator to Target.
    cumulative_bytes_from_target	uint64	The count of bytes since the connection was initiated, from Target to Initiator.
    cumulative_packets_from_target	uint64	The count of packets since the connection was initiated, from Target to Initiator.

Custom/Meta Fields
instance_crn :
network_interface_id :
vpc_crn :
state :
attached_endpoint_type :
version :
collector_crn :
key :
_app :
capture_start_time :
capture_end_time :
"""

import json

LOG_SCHEMA = "version start_time end_time account_id subnet_id instance_id direction initiator_ip initiator_port " \
             "target_ip target_port protocol packets_from_initiator bytes_from_initiator action state"


def iana_protocol_to_str(number):
    """
    reference: https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
    """
    iana_protocol_numbers = {
        "17": "UDP",
        "6": "TCP"
    }
    return iana_protocol_numbers.get(str(number))


class FlowLog:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.protocol = iana_protocol_to_str(self.transport_protocol)
        self.account_id = self.key.split("/")[1].split("=")[1]
        self.subnet_id = self.key.split("/")[4].split("=")[1].split("%3A")[-1]
        self.instance_id = self.instance_crn.split("/")[-1].split(":")[-1]

    def build_log(self, log_schema):
        _log = ""
        for item in log_schema.split():
            if self.__dict__.get(item) is not None:
                _log += str(self.__dict__.get(item)) + " "
            else:
                _log += "Null" + " "
        return _log


def convert_log_plain(out_str):
    if out_str:
        log = FlowLog(**out_str)
        return log.build_log(LOG_SCHEMA)
    else:
        return out_str


if __name__ == "__main__":
    with open("sample.log", "r") as f:
        data = f.read()

    json_data = json.loads(data)
    print(convert_log_plain(json_data))
