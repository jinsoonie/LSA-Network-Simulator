import sys
import json
import threading
import socket
import time

BUFSIZE = 4096

class Neighbor:
    """Represent neighbor node as they are added/removed"""
    def __init__(self, uuid, hostname, backend_port, metric, name=None):
        self.uuid = uuid
        self.hostname = hostname
        self.backend_port = backend_port
        self.metric = metric
        
        self.name = name            # no name by default (need to get through LSA)

        self.is_active = False      # keep track of whether this neighbor is active
        self.last_keepalive = 0.0   # time elapsed since last keepalive 
        self.keepalive_misses = 0   # if 3 consecutive missed, mark as inactive

class ContentServer:
    """maintain info from config and state of this server instance"""
    def __init__(self, config_file):
        # static config from config_file
        self.uuid = None
        self.name = None
        self.backend_port = None

        # dynamic aspects of server
        self.neighbors = {}     # uuid -> Neighbor obj
        # lock to ensure modify/check neighbors atomically
        self.neighbors_lock = threading.RLock()

        # maintain a network_map for each server (for LSA, Dijkstra's)
        self.network_map = {}   # name -> {name -> metric}
        self.node_names = {}    # uuid -> name mapping
        self.lsa_sequence = {}  # uuid -> last seq #
        # lock to ensure modify/check network_map atomically
        self.network_map_lock = threading.RLock()

        # UDP socket (assume UDP used for both send/receive)
        self.socket = None

        # keep track of own LSA sequence number
        self.own_sequence = 0

        # running flag for threads (they use while self.running)
        self.running = True

        # finally, parse received config_file for this server instance
        self.parse_config(config_file)

        # timer for keepalive
        self.KEEPALIVE_INTERVAL = 3 # 3 secs

        # FIX: added LSA timestamp to only keep active nodes in "map"
        # i.e. network_map should only contain active nodes
        self.lsa_timestamp = {}  # uuid -> last update time
        self.LSA_TIMEOUT = 10  # remove LSAs older than 10 seconds (3 sec for each KEEPALIVE interval x 3 + 1 grace sec = 10)

    def parse_config(self, config_file):
        """parse configuration file and populate static data"""
        try:
            with open(config_file, 'r') as f:
                peer_count = 0
                for line in f:
                    # get rid of whitespace, endl chars
                    line = line.strip()

                    # skip if empty line or not '='
                    if not line or '=' not in line:
                        continue
                    
                    # split into key, value fields
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # populate based on key and value
                    if key == 'uuid':
                        self.uuid = value
                    elif key == 'name':
                        self.name = value
                    elif key == 'backend_port':
                        self.backend_port = int(value)
                    elif key == 'peer_count':
                        peer_count = int(value)
                    elif key.startswith('peer_'):
                        parts = [p.strip() for p in value.split(',')]
                        if len(parts) == 4:
                            neighbor = Neighbor(
                                uuid=parts[0],
                                hostname=parts[1],
                                backend_port=int(parts[2]),
                                metric=int(parts[3])
                            )
                            # add the listed peer as a neighbor to neighbors dict
                            self.neighbors[parts[0]] = neighbor
            
            if not all([self.uuid, self.name, self.backend_port]):
                raise ValueError("Missing required configuration fields")
                
        except Exception as e:
            print(f"Error parsing configuration file: {e}", file=sys.stderr)
            sys.exit(1)

    def start(self):
        """start background threads to handle 
        1) receive ALL UDP msgs and forward for correct handling
        2) send keepalive (UDP) to neighbors
        3) check keepalive for neighbors
        4) send lsa broadcast (UDP) to neighbors"""

        # first create UDP socket and bind to this server's port (like udpserver.py)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # use this line to prevent ([Errno 98] Address already in use)
        self.socket.bind(('', self.backend_port))       # '' for HOST means bind to all available
        self.socket.settimeout(1.0)                     # 1 second timeout for checking self.running

        # create 4 daemon threads (background threads)
        self.receiver_thread = threading.Thread(target=self.receive_udp_loop, daemon=True)
        self.keepalive_sender_thread = threading.Thread(target=self.keepalive_sender_loop, daemon=True)
        self.keepalive_checker_thread = threading.Thread(target=self.keepalive_checker_loop, daemon=True)
        self.lsa_sender_thread = threading.Thread(target=self.lsa_sender_loop, daemon=True)
    
        # start 4 daemon threads
        self.receiver_thread.start()
        self.keepalive_sender_thread.start()
        self.keepalive_checker_thread.start()
        self.lsa_sender_thread.start()

        # print("all background threads started")

    def kill(self):
        # by setting running = false, stops all threads
        self.running = False

        if self.socket:
            self.socket.close()

    ### THIS THREAD OVERLOADED when >2 devices, probably want to consider having separate receiver threads for diff messages?
    ### FIX: NEED TO DECOUPLE packet receiving FROM packet processing
    ### receive_udp_loop thread should never make a blocking sendto call.
    def receive_udp_loop(self):
        """background thread that keeps receiving UDP messages until server not running"""
        while (self.running):
            try:
                data, addr = self.socket.recvfrom(BUFSIZE)  # receive raw bytes of BUFSIZE from UDP socket

                # spawn off a background thread to process each message concurrently 
                # (this background thread will also end up spawning off other threads for handle_lsa > forward_lsa)
                # (handle_addneighbor_msg > trigger_lsa_broadcast)
                processing_thread = threading.Thread(
                    target=self.process_message,
                    args=(data, addr),
                    daemon=True
                )
                processing_thread.start()

            except socket.timeout:
                # print("check server still self.running!")
                continue    # check if still self.running again
            except Exception as e:
                if self.running:
                    pass  # ignore connection reset errrors as logn as running
    
    def process_message(self, data, addr):
        """decode received raw data UDP msg, convert msg string into python dict, and determine how to handle"""
        data_string = data.decode()

        # parse data_string back into python dict form (python dict shouldve been packed at sender)
        data_dict = json.loads(data_string)
        msg_type = data_dict.get("type")

        # 3 possible types of messages to be received
        if (msg_type == "keepalive"):
            self.handle_keepalive(data_dict, addr)
        elif (msg_type == "lsa"):
            self.handle_lsa(data_dict, addr)
        elif (msg_type == "addneighbor"):
            self.handle_addneighbor_msg(data_dict, addr)
        else:
            print("Invalid message type received! Not keepalive/lsa/addneighbor")

    def handle_keepalive(self, data_dict, addr):
        """handle incoming keepalive message from other neighbors"""
        sender_uuid = data_dict.get('uuid')
        sender_name = data_dict.get('name')

        with self.neighbors_lock:
            # use extracted sender uuid and name to mark that neighbor as active
            if sender_uuid in self.neighbors:
                neighbor_to_update = self.neighbors[sender_uuid]

                neighbor_to_update.is_active = True     # neighbor marked as active
                neighbor_to_update.last_keepalive = time.time()
                neighbor_to_update.keepalive_misses = 0 # reset misses counter for this neighbor

                # print(f"Keepalive RECEIVED from {neighbor_to_update.name} at t={neighbor_to_update.last_keepalive}")

                # update name if received
                if sender_name:
                    neighbor_to_update.name = sender_name
                    # FIX: Acquire the correct lock to safely modify the shared node_names map
                    with self.network_map_lock:
                        self.node_names[sender_uuid] = sender_name

    def handle_lsa(self, data_dict, addr):
        """handle incoming lsa message from other neighbors"""
        source_uuid = data_dict.get('source_uuid')
        source_name = data_dict.get('source_name')
        sequence = data_dict.get('sequence', -1)    # default -1 as an absurdity val (should never happen)
        neighbors_info = data_dict.get('neighbors', {})

        with self.network_map_lock:
            # check seq number of lsa msg
            if source_uuid in self.lsa_sequence:
                if sequence <= self.lsa_sequence[source_uuid]:
                    return  # msg discarded if STRICTLY older or duplicate
            
            # update seq number of that source uuid
            self.lsa_sequence[source_uuid] = sequence
            # ADDED: since received lsa from other neighbor, mark updated LSA time for that neighbor 
            self.lsa_timestamp[source_uuid] = time.time()

            # update node name mapping
            if source_name:
                self.node_names[source_uuid] = source_name

            # update network map (very important - updates 'network_map' but does NOT run Dijkstra's here)
            # in a true LSA, should run Dijkstra's after every LSA, but lazy impl here
            if source_name:
                self.network_map[source_name] = {}
                for neighbor_name, metric in neighbors_info.items():
                    self.network_map[source_name][neighbor_name] = metric
            
            # fwding here or before?
        
        # NEW FIX - Offload forwarding to a new thread, so receive_udp_loop isnt bogged down by forwarding network IO
        # ALSO, moved to outside the network_map_lock
        forwarding_thread = threading.Thread(
            target=self.forward_lsa, 
            args=(data_dict, addr),
            daemon=True  # Make it a daemon thread
        )
        forwarding_thread.start()

    def handle_addneighbor_msg(self, data_dict, addr):
        """handle incoming add_neighbor request message"""
        sender_uuid = data_dict.get('uuid')
        sender_name = data_dict.get('name')
        sender_port = data_dict.get('backend_port')
        metric = data_dict.get('metric')
        
        # if not already a neighbor, add to neighbor list
        if sender_uuid and sender_uuid not in self.neighbors:
            with self.neighbors_lock:
                new_neighbor = Neighbor(
                    uuid=sender_uuid,
                    hostname=addr[0],  # Use sender's IP
                    backend_port=sender_port,
                    metric=metric,
                    name=sender_name
                )
                self.neighbors[sender_uuid] = new_neighbor
                
            # fix - Offload LSA broadcast to a new thread
            # this allows receiver thread to immediately go back to listening
            lsa_thread = threading.Thread(target=self.trigger_lsa_broadcast, daemon=True)
            lsa_thread.start()

    def trigger_lsa_broadcast(self):
        """Helper function (called by daemon thread) to safely increment sequence and send LSA, don't bog down receive_udp_loop"""
        # use a lock to ensure sequence number is incremented atomically
        with self.network_map_lock: # Or another suitable lock
            self.own_sequence += 1
        self.send_lsa_to_all()

    def keepalive_sender_loop(self):
        """every 3s, send keepalive msgs to all neighbors"""
        while self.running:
            with self.neighbors_lock:
                for neighbor in self.neighbors.values():
                    keepalive_message = {
                        "type": "keepalive",
                        "uuid": self.uuid,
                        "name": self.name
                    }
                    self.send_message(neighbor, keepalive_message)
                    # print(f"Keepalive sent to {neighbor.name}")
            
            time.sleep(self.KEEPALIVE_INTERVAL) # send every 3s

    def keepalive_checker_loop(self):
        """every 3s, check for inactive neighbors by counting their misses"""
        while self.running:
            time.sleep(self.KEEPALIVE_INTERVAL)     # check every 3s

            topology_changed = False                # topology changes if neighbor goes inactive
            with self.neighbors_lock:
                for neighbor in self.neighbors.values():
                    if neighbor.is_active:          # check if keepalive received for this 'active' neighbor
                        time_since_last = time.time() - neighbor.last_keepalive
                        if time_since_last > self.KEEPALIVE_INTERVAL + 1: # grace period of 1 second
                            neighbor.keepalive_misses += 1
                        
                        if neighbor.keepalive_misses >= 3:
                            neighbor.is_active = 0  # neighbor is now marked inactive
                            topology_changed = True
                            # print(f"neighbor {neighbor.name} marked as inactive, their keepalive miss >=3")
            
            # if any neighbor went inactive, topology changed so trigger LSA
            if topology_changed:
                threading.Thread(target=self.trigger_lsa_broadcast, daemon=True).start()

    def cleanup_stale_lsas(self):
        """remove LSAs that haven't been updated recently"""
        with self.network_map_lock:
            current_time = time.time()
            stale_nodes = []
            
            for uuid, timestamp in list(self.lsa_timestamp.items()):
                if current_time - timestamp > self.LSA_TIMEOUT:
                    stale_nodes.append(uuid)
            
            for uuid in stale_nodes:
                if uuid in self.node_names:
                    node_name = self.node_names[uuid]
                    if node_name in self.network_map:
                        del self.network_map[node_name]
                    del self.node_names[uuid]
                if uuid in self.lsa_sequence:
                    del self.lsa_sequence[uuid]
                if uuid in self.lsa_timestamp:
                    del self.lsa_timestamp[uuid]

    def lsa_sender_loop(self):
        """send LSA (periodically) every 3s to all active neighbors"""
        while self.running:
            ## ADDED need to inc self.own_sequence before lsa_to_all
            # use a lock to ensure sequence number is incremented atomically
            with self.network_map_lock: # Or another suitable lock
                self.own_sequence += 1

            # ADDED - remove stale lsas from network_map before sending lsas
            self.cleanup_stale_lsas()
            
            self.send_lsa_to_all()
            time.sleep(3)   # LSA every 3s (periodically)

    def send_lsa_to_all(self):
        """send LSA to ALL neighbors"""
        # build active neighbor information (neighbors info should only contain info about ACTIVE neighbors)
        neighbors_info = {}
        
        with self.neighbors_lock:
            for neighbor in self.neighbors.values():
                if neighbor.is_active and neighbor.name:
                    neighbors_info[neighbor.name] = neighbor.metric

        # FIX ADD: update this node's own entry in network map immediately (not just neighbors)
        with self.network_map_lock:
            self.network_map[self.name] = dict(neighbors_info)
        
        lsa_message = {
            'type': 'lsa',
            'source_uuid': self.uuid,
            'source_name': self.name,
            'sequence': self.own_sequence,  # incl own_sequence num
            'neighbors': neighbors_info     # incl new neighbors state
        }
        
        # send to ALL neighbors  
        ### FIX: SHOULD IT BE ALL neighbors, NOT JUST ACTIVE? (Because might have unintent marked as inactive due to network traffic)
        ### is it ok to send lsa ad to inactive? Yes because theoretically if really inactive lsa doesnt matter?
        with self.neighbors_lock:
            for neighbor in self.neighbors.values():
                # if neighbor.is_active:   commented out for send to ALL neighbors (lsa info however contains only active neighbors)
                    try:
                        self.send_message(neighbor, lsa_message)
                    except:
                        pass

    def forward_lsa(self, lsa_message, sender_addr):
        """forward lsa to all neighbors except sender which received from"""
        # print(f"{self.name}: as Forwarding LSA, OG sender was {sender_addr}")
        with self.neighbors_lock:
            for neighbor in self.neighbors.values():
                # don't send back to neighbor that sent the lsa to us
                # if neighbor.is_active and (neighbor.hostname, neighbor.backend_port) != sender_addr:
                if (neighbor.hostname, neighbor.backend_port) != sender_addr:
                    self.send_message(neighbor, lsa_message)
            
    def send_message(self, neighbor, message):
        """send a message to a specific neighbor, where message is a python dict type, neighbor is Neighbor obj type"""
        if self.socket:
            data = json.dumps(message).encode()
            # taken from udpclient.py sendto
            self.socket.sendto(data, (neighbor.hostname, neighbor.backend_port))

    def handle_command(self, command):
        """process a command received through stdin appropriately"""
        command = command.strip()

        if command == "uuid":
            return {"uuid": self.uuid}
        
        elif command == "neighbors":
            return self.handle_neighbors_command()
        
        elif command == "map":
            return self.handle_map_command()
        
        elif command == "rank":
            return self.handle_rank_command()
        
        elif command.startswith("addneighbor"):
            self.handle_addneighbor_command(command)
            return None

        elif command == "kill":
            self.kill()
            sys.exit(0)

        else:
            return None     # unknown command prints nothing
    
    def handle_neighbors_command(self):
        result = {"neighbors": {}}

        with self.neighbors_lock:
            # loop through neighbors dict (consists of uuid as key, neighbor object as val)
            for uuid, neighbor in self.neighbors.items():
                # if neighbor.is_active and neighbor.name:    # neighbor need to be active and known name
                if neighbor.is_active:
                    result["neighbors"][neighbor.name] = {
                        "uuid": neighbor.uuid,
                        "host": neighbor.hostname,
                        "backend_port": int(neighbor.backend_port),
                        "metric": int(neighbor.metric)
                    }

        return result
    
    def handle_map_command(self):
        result = {"map": {}}

        with self.network_map_lock:
            # loop through network_map dict (consists of node as key, and its connections (dict form) as val)
            for node, node_connections in self.network_map.items():
                # FIX: only include node_connections if not empty (i.e. only incl nodes that have at least one neighbor)
                if node_connections:
                    result["map"][node] = dict(node_connections)   #  make sure use dict() to create a copy! Not ref to original

        return result

    def handle_rank_command(self):
        """need to compute shortest paths (dijkstra's) based on current self.network_map"""
        result = {"rank": {}}

        with self.network_map_lock:
            # safeguard: handle empty map case first
            if not self.network_map:
                return result
            
            distances = self.compute_shortest_paths()   # perform dijkstra's given current network_map

            # fix: filter so only include reachable nodes (finite distance, exclude self)
            for node, dist in distances.items():
                if node != self.name and dist != float('inf'):
                    result["rank"][node] = dist

        return result
    
    def handle_addneighbor_command(self, command):
        """take complete addneighbor command and parse to add neighbor"""
        parts = command.split()
        
        args = {}
        # parts[1:] should consist of uuid, host, backend_port, metric
        for part in parts[1:]:
            if '=' in part:
                key, value = part.split('=', 1)
                args[key] = value
        
        with self.neighbors_lock:
            new_neighbor = Neighbor(
                uuid=args['uuid'],
                hostname=args['host'],
                backend_port=int(args['backend_port']),
                metric=int(args['metric'])
            )
            self.neighbors[args['uuid']] = new_neighbor
        
        # send add_neighbor message to new neighbor 
        message = {
            'type': 'addneighbor',
            'uuid': self.uuid,
            'name': self.name,
            'backend_port': self.backend_port,
            'metric': args.get('metric')
        }
        self.send_message(new_neighbor, message)
        
        # inc sequence and trigger LSA for everybody else
        threading.Thread(target=self.trigger_lsa_broadcast, daemon=True).start()

    def compute_shortest_paths(self):
        """Dijkstra's on current self.network_map 
        (not a TRUE link-state, bc this func (Dijkstra's) run only on Rank command)
        i.e. nodes just keep track of updated 'network_map' but don't actually run Dijkstra's until Rank command"""
        if self.name not in self.network_map:
            return {}
        
        # init distances as dict, where dist to all other nodes are inf, init dist to self as 0
        distances = {node: float('inf') for node in self.network_map}
        distances[self.name] = 0
        visited = set()

        # loop until all nodes visited
        while len(visited) < len(distances):
            # first subloop: find curr min_node (1st min_node will be starter pt bc distances[self] = 0)
            min_dist = float('inf')
            min_node = None
            for node in distances:
                if node not in visited and distances[node] < min_dist:
                    min_dist = distances[node]
                    min_node = node

            # if no min_node, then no more nodes to visit
            if min_node == None:
                break
            
            # mark as curr min_node as visited
            visited.add(min_node)

            # second subloop: update paths to neighbors of curr min_node as necessary
            if min_node in self.network_map:
                for neighbor, metric in self.network_map[min_node].items():
                    if neighbor in distances:
                        # print(f"metric is: {metric}, and it is {type(metric)}")
                        alt_dist_to_neighbor = distances[min_node] + int(metric)
                        if (alt_dist_to_neighbor < distances[neighbor]):
                            distances[neighbor] = alt_dist_to_neighbor
    
        return distances


if __name__ == '__main__':
    # parse command line args first
    if len(sys.argv) != 3 or sys.argv[1] != '-c':
        print("Usage: python3 content_server.py -c <conf_file>", file=sys.stderr)
        sys.exit(1)
    else:
        conf_file = sys.argv[2]

    # Create and start the server
    server = ContentServer(conf_file)
    server.start()
    
    # main command loop - wait for stdin commands
    while True:
        try:
            command = input()
            response = server.handle_command(command)
            
            # only output if this command has a response
            if response is not None:
                print(response)
                
        except EOFError:
            server.kill()
            break
        except KeyboardInterrupt:
            server.kill()
            break
    
    # Clean shutdown
    server.kill()
    sys.exit(0)
