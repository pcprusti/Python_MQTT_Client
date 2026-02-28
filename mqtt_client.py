
import socket
import struct
import time
import traceback
import threading

class MQTTClient:
    def __init__(self, broker_host, broker_port=1883, client_id="python_client"):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.socket = None
        self.connected = False
        self.running = False
        # keepalive settings
        self._keepalive = 60
        self._keepalive_thread = None
        self._keepalive_stop = threading.Event()
        # receiver / pending map
        self._receiver_thread = None
        self._receiver_stop = threading.Event()
        self._pending = {}               # pid -> {'event': Event, 'resp': bytes}
        self._pending_lock = threading.Lock()
        self._message_callback = None
        self._subscriptions = {}         # topic -> qos

    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            print(f"Attempting to connect to {self.broker_host}:{self.broker_port}")
            self.socket.connect((self.broker_host, self.broker_port))
            print("TCP connection established")
            self.socket.settimeout(5)
            packet = self._build_connect_packet()
            print(f"Sending CONNECT packet ({len(packet)} bytes): {packet.hex()}")
            self.socket.sendall(packet)
            time.sleep(0.1)
            # read CONNACK (fixed header + remaining length + 2 bytes)
            response = b""
            try:
                # read first 2 bytes (fixed header + remaining length)
                while len(response) < 2:
                    chunk = self.socket.recv(2 - len(response))
                    if not chunk:
                        print("ERROR: Connection closed by broker before CONNACK")
                        return False
                    response += chunk
                # remaining length (usually 2)
                rem_len = response[1]
                while len(response) < 2 + rem_len:
                    chunk = self.socket.recv(2 + rem_len - len(response))
                    if not chunk:
                        print("ERROR: Connection closed by broker while reading CONNACK")
                        return False
                    response += chunk
                print(f"Received {len(response)} bytes: {response.hex()}")
                if response[0] != 0x20:
                    print(f"ERROR: Invalid packet type {hex(response[0])}")
                    return False
                if len(response) < 4:
                    print("ERROR: Incomplete CONNACK")
                    return False
                return_code = response[3]
                return_messages = {
                    0x00: "Connection Accepted",
                    0x01: "Unacceptable protocol version",
                    0x02: "Identifier rejected",
                    0x03: "Server unavailable",
                    0x04: "Bad username or password",
                    0x05: "Not authorized"
                }
                msg = return_messages.get(return_code, f"Unknown code {return_code}")
                #print(f"CONNACK response: {msg}")
                if return_code == 0x00:
                    self.connected = True
                    print("✓ Connected successfully")
                    # start receiver then keepalive thread
                    self._start_receiver()
                    self._start_keepalive()
                    return True
                else:
                    print(f"CONNACK return code: {return_code}: {msg}")
                    return False
            except socket.timeout:
                print("ERROR: Socket timeout waiting for CONNACK")
                return False
        except ConnectionRefusedError:
            print("ERROR: Connection refused")
            return False
        except Exception as e:
            print(f"ERROR: {e}")
            traceback.print_exc()
            return False

    def _build_connect_packet(self):
        """Build MQTT CONNECT packet (correct: protocol name length + 'MQTT')"""
        protocol_name = b"MQTT"
        protocol_name_len = struct.pack("!H", len(protocol_name))   # <-- required 2-byte length
        protocol_level = b"\x04"    # MQTT 3.1.1
        '''
        The connect flags byte structure is:
        Bit 7: User Name Flag
        Bit 6: Password Flag
        Bit 5: Will Retain
        Bit 4: Will QoS
        Bit 3: Will Flag
        Bit 2: Clean Session
        Bit 1-0: Reserved
        '''
        connect_flags = b"\x02"     # Clean Session = 1
        keep_alive = b"\x00\x3c"    # 60 seconds

        variable_header = protocol_name_len + protocol_name + protocol_level + connect_flags + keep_alive
        client_id_bytes = self.client_id.encode()
        payload = struct.pack("!H", len(client_id_bytes)) + client_id_bytes

        remaining_length = len(variable_header) + len(payload)
        # remaining length small (<128) in our cases
        remaining_length_encoded = bytes([remaining_length]) if remaining_length < 128 else self._encode_remaining_length(remaining_length)
        fixed_header = b"\x10" + remaining_length_encoded
        packet = fixed_header + variable_header + payload

        print("DEBUG Packet Structure:")
        print(f"  Fixed Header:      {fixed_header.hex()}")
        print(f"  Variable Header:   {variable_header.hex()}")
        print(f"  Payload:           {payload.hex()}")
        print(f"  Total Length:      {len(packet)} bytes")
        return packet

    def _next_packet_id(self):
        self._packet_id = getattr(self, "_packet_id", 0) + 1
        if self._packet_id > 0xFFFF:
            self._packet_id = 1
        return self._packet_id

    def _recv_exact(self, n, timeout=5):
        self.socket.settimeout(timeout)
        data = b""
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _recv_packet(self, timeout=5):
        # read first byte
        first = self._recv_exact(1, timeout)
        if not first:
            return None
        # read remaining length (variable byte integer)
        rem_bytes = b""
        multiplier = 1
        value = 0
        while True:
            b = self._recv_exact(1, timeout)
            if not b:
                return None
            rem_bytes += b
            digit = b[0]
            value += (digit & 0x7F) * multiplier
            if (digit & 0x80) == 0:
                break
            multiplier *= 128
        # read remaining payload
        body = self._recv_exact(value, timeout) if value > 0 else b""
        if body is None:
            return None
        return first + rem_bytes + body

    def _parse_remaining_length(self, packet):
        # returns (remaining_length_value, rem_len_bytes_count)
        idx = 1
        multiplier = 1
        value = 0
        count = 0
        while True:
            b = packet[idx]
            count += 1
            value += (b & 0x7F) * multiplier
            if (b & 0x80) == 0:
                break
            multiplier *= 128
            idx += 1
        return value, count

    def _start_receiver(self):
        if self._receiver_thread and self._receiver_thread.is_alive():
            return
        self._receiver_stop.clear()
        self._receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        self._receiver_thread.start()

    def _stop_receiver(self):
        self._receiver_stop.set()
        if self._receiver_thread:
            self._receiver_thread.join(timeout=1)
            self._receiver_thread = None

    def _receiver_loop(self):
        while not self._receiver_stop.is_set() and self.connected:
            try:
                pkt = self._recv_packet(timeout=1)
                if not pkt:
                    continue
                ptype = pkt[0] & 0xF0
                if ptype == 0xD0:  # PINGRESP
                    with self._pending_lock:
                        ev = self._pending.get(('_ping',))
                        if ev:
                            ev['resp'] = pkt
                            ev['event'].set()
                elif ptype == 0x90:  # SUBACK
                    rem_len, rem_count = self._parse_remaining_length(pkt)
                    pid_idx = 1 + rem_count
                    pid = struct.unpack("!H", pkt[pid_idx:pid_idx+2])[0]
                    with self._pending_lock:
                        # don't pop here — set resp and signal; waiting thread will pop
                        #entry = self._pending.pop(pid, None)
                        entry = self._pending.get(pid)
                        if entry:
                            entry['resp'] = pkt
                            entry['event'].set()
                elif ptype == 0x40:  # PUBACK
                    rem_len, rem_count = self._parse_remaining_length(pkt)
                    pid_idx = 1 + rem_count
                    pid = struct.unpack("!H", pkt[pid_idx:pid_idx+2])[0]
                    with self._pending_lock:
                        #entry = self._pending.pop(pid, None)
                        entry = self._pending.get(pid)
                        if entry:
                            entry['resp'] = pkt
                            entry['event'].set()
                elif ptype == 0x50:  # PUBREC
                    rem_len, rem_count = self._parse_remaining_length(pkt)
                    pid_idx = 1 + rem_count
                    pid = struct.unpack("!H", pkt[pid_idx:pid_idx+2])[0]
                    with self._pending_lock:
                        entry = self._pending.get(pid)
                        if entry:
                            entry['resp'] = pkt
                            entry['event'].set()
                elif ptype == 0x70:  # PUBCOMP
                    rem_len, rem_count = self._parse_remaining_length(pkt)
                    pid_idx = 1 + rem_count
                    pid = struct.unpack("!H", pkt[pid_idx:pid_idx+2])[0]
                    with self._pending_lock:
                        entry = self._pending.pop(pid, None)
                        if entry:
                            entry['resp'] = pkt
                            entry['event'].set()
                elif ptype == 0x30:  # PUBLISH
                    # parse remaining length to find variable header start
                    rem_len, rem_count = self._parse_remaining_length(pkt)
                    vh_idx = 1 + rem_count
                    topic_len = struct.unpack("!H", pkt[vh_idx:vh_idx+2])[0]
                    topic = pkt[vh_idx+2:vh_idx+2+topic_len].decode()
                    offset = vh_idx+2+topic_len
                    qos = (pkt[0] & 0x06) >> 1
                    packet_id = None
                    if qos > 0:
                        packet_id = struct.unpack("!H", pkt[offset:offset+2])[0]
                        offset += 2
                    payload = pkt[offset:]
                    # deliver to callback
                    if self._message_callback:
                        try:
                            self._message_callback(topic, payload, qos)
                        except Exception:
                            pass
                    else:
                        print(f"[PUBLISH] topic={topic} qos={qos} payload={payload}")
                    # send required ACKs
                    if qos == 1:
                        # PUBACK
                        if packet_id is not None:
                            self.socket.sendall(b"\x40\x02" + struct.pack("!H", packet_id))
                    elif qos == 2:
                        # respond PUBREC, later expect PUBREL then send PUBCOMP
                        if packet_id is not None:
                            self.socket.sendall(b"\x50\x02" + struct.pack("!H", packet_id))
                elif ptype == 0x62:  # PUBREL (from broker to client) - treat and respond with PUBCOMP
                    rem_len, rem_count = self._parse_remaining_length(pkt)
                    pid_idx = 1 + rem_count
                    pid = struct.unpack("!H", pkt[pid_idx:pid_idx+2])[0]
                    # send PUBCOMP
                    self.socket.sendall(b"\x70\x02" + struct.pack("!H", pid))
                else:
                    # ignore others
                    pass
            except Exception:
                # transient read errors, continue loop
                continue

    def publish(self, topic, message, qos=0, timeout=5):
        """Publish message to topic with QoS 0, 1, or 2."""
        if not self.connected:
            print("Not connected")
            return False
        try:
            topic_bytes = topic.encode()
            msg_bytes = message.encode() if isinstance(message, str) else message

            # Variable header: topic name
            vh_topic = struct.pack("!H", len(topic_bytes)) + topic_bytes

            if qos == 0:
                fixed_byte = 0x30  # PUBLISH, QoS0
                remaining_length = len(vh_topic) + len(msg_bytes)
                remaining = bytes([remaining_length]) if remaining_length < 128 else self._encode_remaining_length(remaining_length)
                packet = bytes([fixed_byte]) + remaining + vh_topic + msg_bytes
                self.socket.sendall(packet)
                return True

            # For QoS 1 and 2 include Packet Identifier
            pid = self._next_packet_id()
            pid_bytes = struct.pack("!H", pid)
            vh = vh_topic + pid_bytes
            remaining_length = len(vh) + len(msg_bytes)
            remaining = bytes([remaining_length]) if remaining_length < 128 else self._encode_remaining_length(remaining_length)
            fixed_byte = 0x30 | (qos << 1)
            packet = bytes([fixed_byte]) + remaining + vh + msg_bytes
            self.socket.sendall(packet)

            if qos == 1:
                # wait for PUBACK via pending
                ev = threading.Event()
                with self._pending_lock:
                    self._pending[pid] = {'event': ev, 'resp': None}
                ok = ev.wait(timeout)
                with self._pending_lock:
                    entry = self._pending.pop(pid, None)
                return ok and entry and (entry['resp'] is not None)

            if qos == 2:
                # wait for PUBREC
                ev1 = threading.Event()
                with self._pending_lock:
                    self._pending[pid] = {'event': ev1, 'resp': None}
                ok1 = ev1.wait(timeout)
                if not ok1:
                    with self._pending_lock:
                        self._pending.pop(pid, None)
                    return False
                # send PUBREL
                self.socket.sendall(b"\x62\x02" + pid_bytes)
                # wait for PUBCOMP
                ev2 = threading.Event()
                with self._pending_lock:
                    self._pending[pid] = {'event': ev2, 'resp': None}
                ok2 = ev2.wait(timeout)
                with self._pending_lock:
                    entry = self._pending.pop(pid, None)
                return ok2 and entry and (entry['resp'] is not None)

            return False
        except Exception as e:
            print(f"Publish failed: {e}")
            return False

    def subscribe(self, topic, qos=0, callback=None, timeout=5):
        """Subscribe to a topic and optionally set a message callback for incoming publishes."""
        print(f"Subscribing to {topic} with QoS {qos}...")
        if not self.connected:
            print("Not connected")
            return False
        try:
            print(f"DEBUG: Building SUBSCRIBE packet for topic '{topic}' with QoS {qos}")
            pid = self._next_packet_id()
            pid_bytes = struct.pack("!H", pid)
            topic_bytes = topic.encode()
            # payload: topic filter + requested QoS
            payload = struct.pack("!H", len(topic_bytes)) + topic_bytes + bytes([qos])
            remaining_length = 2 + len(payload)  # packet id + payload
            remaining = bytes([remaining_length]) if remaining_length < 128 else self._encode_remaining_length(remaining_length)
            fixed = b"\x82"  # SUBSCRIBE (with required flags)
            packet = fixed + remaining + pid_bytes + payload
            # prepare pending event
            ev = threading.Event()
            with self._pending_lock:
                self._pending[pid] = {'event': ev, 'resp': None}
            self.socket.sendall(packet)
            ok = ev.wait(timeout)
            with self._pending_lock:
                entry = self._pending.pop(pid, None)
            if not ok or entry is None or entry['resp'] is None:
                print("ERROR: No SUBACK received")  
                return False
            # register callback and subscription
            if callback:
                self._message_callback = callback
            self._subscriptions[topic] = qos
            print(f"✓ Subscribed to {topic} with QoS {qos}")
            return True
        except Exception as e:
            print(f"Subscribe failed: {e}")
            return False

    def ping(self, timeout=5):
        """Send PINGREQ and wait for PINGRESP."""
        if not self.connected or not self.socket:
            return False
        try:
            # prepare pending event for ping
            ev = threading.Event()
            with self._pending_lock:
                self._pending[('_ping',)] = {'event': ev, 'resp': None}
            # PINGREQ packet
            self.socket.sendall(b"\xc0\x00")
            ok = ev.wait(timeout)
            with self._pending_lock:
                self._pending.pop(('_ping',), None)
            return ok
        except Exception:
            return False

    def _keep_alive_loop(self):
        """Background keep-alive loop: send PINGREQ periodically."""
        # send at half the keepalive interval (or at least every 1s)
        interval = max(1, self._keepalive // 2)
        while not self._keepalive_stop.wait(interval):
            if not self.connected:
                break
            try:
                # send ping, receiver thread will handle PINGRESP
                self.socket.sendall(b"\xc0\x00")
            except Exception:
                try:
                    self.socket.close()
                except:
                    pass
                self.connected = False
                break

    def _start_keepalive(self):
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            return
        self._keepalive_stop.clear()
        self._keepalive_thread = threading.Thread(target=self._keep_alive_loop, daemon=True)
        self._keepalive_thread.start()

    def _stop_keepalive(self):
        self._keepalive_stop.set()
        if self._keepalive_thread:
            self._keepalive_thread.join(timeout=1)
            self._keepalive_thread = None

    def _encode_remaining_length(self, length):
        encoded = bytearray()
        while length > 127:
            encoded.append((length & 0x7F) | 0x80)
            length >>= 7
        encoded.append(length & 0x7F)
        return bytes(encoded)

    def disconnect(self):
        try:
            # stop keepalive and receiver first
            self._stop_keepalive()
            self._stop_receiver()
            if self.socket:
                try:
                    self.socket.sendall(b"\xe0\x00")
                except:
                    pass
                self.socket.close()
        finally:
            self.connected = False
            print("✓ Disconnected")

if __name__ == "__main__":
    client = MQTTClient("localhost", 1883, "python_client")
    if client.connect():
        # subscribe with a callback
        def on_message(topic, payload, qos):
            print(f"MSG <- {topic} qos={qos} payload={payload}")
        ok = client.subscribe("python/test2", qos=1, callback=on_message)
        print("SUBSCRIBE",  "✓ OK" if ok else "FAILED")
        # publish to test
        time.sleep(1)
        client.publish("python/test", "hello", qos=1)
        time.sleep(10)
        ok = client.ping()
        print("PINGRESP received" if ok else "PING failed")
    client.disconnect()
