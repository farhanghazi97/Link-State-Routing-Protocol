import sys
import pickle
import time
import heapq

from datetime import datetime, timedelta
from threading import Thread, Lock, Timer
from socket import socket, AF_INET, SOCK_DGRAM

UPDATE_INTERVAL = 1
ROUTE_UPDATE_INTERVAL = 30
PERIODIC_HEART_BEAT = 0.5
NODE_FAILURE_INTERVAL = 4
TIMEOUT = 15

class ReceiveThread(Thread):

    def __init__(self, name, router_data , thread_lock):
        Thread.__init__(self)
        self.name = name
        self.router_data = router_data
        self.thread_lock = thread_lock
        self.server_socket = socket(AF_INET, SOCK_DGRAM)
        self.packets = set()
        self.LSA_SN = {}
        self.HB_set = {}
        self.LSA_DB = {}
        self.inactive_list = set()

    def run(self):
        self.serverSide()

    def __str__(self):
        return "I am Router {0} with PORT {1} - READY TO RECEIVE".format(
            self.router_data['RID'],
            self.router_data['Port']
        )

    def __del__(self):
        self.server_socket.close()

    def serverSide(self):

        server_name = 'localhost'
        server_port = int(self.router_data['Port'])
        self.server_socket.bind((server_name, server_port))
        inactive_list_size = len(self.inactive_list)

        while True:

            # Socket ready to receive!
            data, client_address = self.server_socket.recvfrom(1024)
            local_copy_LSA = pickle.loads(data)

            # Handle case if message received is a heartbeat message
            if isinstance(local_copy_LSA , list):

                # Get current date and time at which heart beat for
                # respective router was received
                now = datetime.now()
                RID = local_copy_LSA[0]['RID']

                # Update local routers database of heart beat timestamps
                # for each neighbouring router (provided it is still alive)
                if RID not in self.inactive_list:
                    self.HB_set.update({RID : now})

                # Periodically check for any dead neighbours and update
                # inactive list of routers
                Timer(NODE_FAILURE_INTERVAL , self.checkForNodeFailure).start()

                # If the list of inactive routers is ever updated, we must transmit
                # a new LSA to notify other routers of the update to the topology
                if len(self.inactive_list) > inactive_list_size:

                    # Update this router's list of neighbours using inactive list
                    self.updateNeighboursList()

                    # If new routers have been declared dead, we need to transmit
                    # a fresh LSA with updated neighbour information
                    self.transmitNewLSA()

                    # Clear the set so that the fresh set
                    # will only track active neighbours
                    self.HB_set.clear()

                    # Store new size of inactive list
                    inactive_list_size = len(self.inactive_list)

            # Handle case if the message received is an LSA
            else:

                # Grab list of neighbouring routers of router that sent this LSA
                neighbour_routers = self.router_data['Neighbours Data']

                # Grab 'FLAG' field from LSA received
                flag = local_copy_LSA['FLAG']

                # Append this router's ID to LSA_SN database
                self.LSA_SN.update({self.router_data['RID'] : 0})

                # Any new LSA received that have not been seen before are stored within this
                # routers local link-state database
                if local_copy_LSA['RID'] not in self.packets:
                    for router in neighbour_routers:
                        if router['NID'] != local_copy_LSA['RID']:
                            self.packets.add(local_copy_LSA['RID'])
                            self.LSA_SN.update({local_copy_LSA['RID']: local_copy_LSA['SN']})
                            self.LSA_DB.update({local_copy_LSA['RID'] : local_copy_LSA})
                            # If the LSA received does not exist within router database , forward it to neighbours
                            # If LSA exists within database, do not forward it (silently drop it)
                            self.server_socket.sendto(
                                pickle.dumps(self.LSA_DB[local_copy_LSA['RID']]),
                                (server_name, int(router['Port']))
                            )
                            time.sleep(1)
                    # Update global graph using constructed link-state database
                    self.updateGraph(graph, self.LSA_DB, 0)

                # If a router is removed from the topology, we receive an updated LSA
                # which we use to update the graph network.
                # (ALL UPDATED LSA HAVE A UNIQUE 'FLAG' WITH VALUE 1 TO IDENTIFY THEM)
                if flag is 1:
                    # If the LSA received has a SN number that is greater than the existing record of
                    # SN for that router, we can confirm that the LSA received is a fresh LSA
                    if local_copy_LSA['SN'] > self.LSA_SN[local_copy_LSA['RID']]:
                        self.LSA_SN.update({local_copy_LSA['RID'] : local_copy_LSA['SN']})
                        self.LSA_DB.update({local_copy_LSA['RID'] : local_copy_LSA})
                        # If the new LSA has any router listed as inactive (i.e dead) we remove these explicitly from
                        # the topology so that they are excluded from future shortest path calculations
                        if local_copy_LSA['DEAD']:
                            self.updateLSADB(local_copy_LSA['DEAD'])
                            self.updateGraphOnly(graph, local_copy_LSA['DEAD'])
                        # Send the new LSA received back to the sending router (so as to ensure that it is a two-way
                        # update for the sender and recipient's local database)
                        self.server_socket.sendto(
                            pickle.dumps(self.LSA_DB[local_copy_LSA['RID']]),
                            (server_name, int(local_copy_LSA['Port']))
                        )
                        time.sleep(1)
                    else:
                        # If old data is being received, that is, there is no new LSA, we simply forward the message
                        # onto our neighbours (now with the list of updated neighbours and higher SN)
                        for new_router in self.router_data['Neighbours Data']:
                            if new_router['NID'] != self.router_data['RID']:
                                try:
                                    self.server_socket.sendto(
                                        pickle.dumps(self.LSA_DB[local_copy_LSA['RID']]),
                                        (server_name, int(new_router['Port']))
                                    )
                                except KeyError:
                                    pass
                            time.sleep(1)
                    # After getting a fresh LSA, we wait for sometime (so that the global graph can update) and then
                    # recompute shortest paths using Dijkstra algorithm
                    Timer(10, self.updateGraphAfterFailure, [
                        graph,
                        self.inactive_list,
                        self.LSA_DB,
                        1,
                        self.thread_lock]
                    ).start()

    # Helper function to update the global graph when a router
    # in the topology fails
    def updateGraphOnly(self, graph_arg, dead_list):

        for node in graph_arg:
            if node[0] in dead_list:
                graph_arg.remove(node)
            if node[1] in dead_list:
                graph_arg.remove(node)

    # Update this router's local link-state database
    # after a router fails
    def updateLSADB(self, lsa_db):

        for node in lsa_db:
            if node in self.LSA_DB:
                del self.LSA_DB[node]

    # Period function that runs in the HeartBeat Thread
    # Used to check for any failed nodes in the topology
    def checkForNodeFailure(self):

        current_time = datetime.now()
        td = timedelta(seconds=TIMEOUT)

        for node in self.HB_set:
            difference = current_time - self.HB_set[node]
            if difference > td:
                if node not in self.inactive_list:
                    self.inactive_list.add(node)

    # Helper function to update this router's list
    # of active neighbours after a router fails
    def updateNeighboursList(self):

        for node in self.router_data['Neighbours Data']:
            if node['NID'] in self.inactive_list:
                self.router_data['Neighbours Data'].remove(node)

    # Triggered by all active neighbouring routers
    # when a neighbour to them fails
    def transmitNewLSA(self):

        server_name = 'localhost'
        updated_router_information = {}

        updated_router_information['RID'] = self.router_data['RID']
        updated_router_information['Port'] = self.router_data['Port']
        updated_router_information['Neighbours'] = self.router_data['Neighbours'] - 1
        updated_router_information['Neighbours Data'] = self.router_data['Neighbours Data']

        self.router_data['SN'] = self.router_data['SN'] + 1
        updated_router_information['SN'] = self.router_data['SN']

        updated_router_information['FLAG'] = 1
        updated_router_information['DEAD'] = self.inactive_list

        new_data = pickle.dumps(updated_router_information)

        for router in self.router_data['Neighbours Data']:
            self.server_socket.sendto(new_data , (server_name , int(router['Port'])))
        time.sleep(1)


    def updateGraphAfterFailure(self, *args):

        if args[3] is 1:
            try:
                for node in args[2]:
                    if args[2][node]['RID'] in args[1]:
                        del args[2][node]
            except RuntimeError:
                pass

        for node in args[0]:
            if node[0] in args[1]:
                args[0].remove(node)
            if node[1] in args[1]:
                args[0].remove(node)

        for node in args[2]:
            for router in args[2][node]['Neighbours Data']:
                if router['NID'] in args[1]:
                    args[2][node]['Neighbours Data'].remove(router)

        self.updateGraph(args[0], args[2], 1)

    # Helper function that builds a useful data structure
    # that will in turn be used by another helper function
    # to construct an adjacency list from the global graph.
    # The adjacency list is then in turn used by the
    # Dijkstra function to compute shortest path
    def updateGraph(self, graph_arg, lsa_data, flag):

        if flag is 1:

            graph.clear()

        for node in lsa_data:

            source_node = lsa_data[node]['RID']
            neighbours_dict = lsa_data[node]['Neighbours Data']
            neighbours_list = []

            for neighbour in neighbours_dict:
                if (source_node < neighbour['NID']):
                    graph_data = [source_node, neighbour['NID'], neighbour['Cost'], neighbour['Port']]
                else:
                    graph_data = [neighbour['NID'], source_node, neighbour['Cost'], neighbour['Port']]
                neighbours_list.append(graph_data)

            for node in neighbours_list:
                exists = False
                for graph_node in graph_arg:
                    if node[0] == graph_node[0] and node[1] == graph_node[1]:
                        exists = True
                        break
                if exists is False:
                    graph_arg.append(node)

        # Get adjacency list and list of graph nodes
        adjacency_list , graph_nodes = self.organizeGraph(graph_arg)

        # Run Dijkstra's algorithm periodically
        Timer(ROUTE_UPDATE_INTERVAL, self.runDijkstra, [adjacency_list, graph_nodes]).start()

    # Uses the global graph to construct a adjacency list
    # (represented using python 'dict') which in turn is
    # used by the Dijkstra function to compute shortest paths
    def organizeGraph(self, graph_arg):

        # Set to contain nodes within graph
        nodes = set()

        # Determine nodes in entire topology
        # and update set of nodes
        for node in graph_arg:
            if node[0] not in nodes:
                nodes.add(node[0])
            if node[1] not in nodes:
                nodes.add(node[1])

        # Sort nodes alphabetically
        sorted_nodes = sorted(nodes)

        # Create dict to store all edges between
        # vertices as an adjacency list
        new_LL = dict()
        for node in sorted_nodes:
            new_LL[node] = dict()

        # Using all link-state advertisement received
        # from all nodes, create the initial adjacency list
        # based solely on data received from neighbours
        for node in sorted_nodes:
            for link in graph_arg:
                if node == link[0]:
                    new_LL[node].update({link[1] : link[2]})

        # Update adjacency list so as to reflect all outgoing/incoming
        # links (Graph should now fully represent the network topology
        for node in sorted_nodes:
            for source_node , cost in new_LL[node].items():
                new_LL[source_node].update({node : cost})

        # Return adjacency list and least_cost_path dict
        # to use for Dijkstra Computation
        return (new_LL , sorted_nodes)

    # Runs Dijkstra's algorithm on the given adjacency list
    # and prints out the shortest paths. Makes use of
    # python's heapq
    def runDijkstra(self, *args):

        # Use each router ID as start vertex for algorithm
        start_vertex = self.router_data['RID']
        # Initially, distances to all vertices (except source) is infinity
        distances = {vertex: float('infinity') for vertex in args[0]}
        # Distance to source node is 0
        distances[start_vertex] = 0

        # Create a least cost path dict to be updated using
        # Dijkstra calculation
        least_cost_path = {}
        for node in args[0]:
            least_cost_path[node] = []

        # Add start vertex to priority queue
        pq = [(0 , start_vertex)]
        while len(pq) > 0:
            # Pop item from queue and grab distance and vertex ID
            current_distance , current_vertex = heapq.heappop(pq)
            if current_distance > distances[current_vertex]:
                continue
            for n , w in args[0][current_vertex].items():
                # Round path cost to 1 d.p
                distance = round((current_distance + w) , 1)
                # If aggregated cost is less than current known cost,
                # update cost to that vertex
                if distance < distances[n]:
                    distances[n] = distance
                    least_cost_path[n].append(current_vertex)
                    # Push next neighbour onto queue
                    heapq.heappush(pq , (distance , n))

        # Finalise path array
        final_paths = []
        for node in args[0]:
            path_string = ""
            if node != self.router_data['RID']:
                end_node = node
                while(not (path_string.endswith(self.router_data['RID']))):
                    temp_path = least_cost_path[node][-1]
                    path_string = path_string + temp_path
                    node = temp_path
                path_string = (path_string)[::-1] + end_node
                final_paths.append(path_string)

        # Display final output after Dijkstra computation
        self.showPaths(final_paths , distances , self.router_data['RID'])

    def showPaths(path, graph_nodes, distances, source_node):

        # Delete source node from list of paths
        del distances[source_node]

        # Print router ID
        print("I am Router {0}".format(source_node))

        index = 0
        # Display output for dijkstra
        for vertex in distances:
            print("Least cost path to router {0}:{1} and the cost is {2}".format(
                vertex,
                graph_nodes[index],
                distances[vertex])
            )
            index = index + 1
        print()

class SendThread(Thread):

    def __init__(self, name, router_data , thread_lock):
        Thread.__init__(self)
        self.name = name
        self.router_data = router_data
        self.thread_lock = thread_lock
        self.client_socket = socket(AF_INET, SOCK_DGRAM)

    def run(self):
        self.clientSide()

    def __str__(self):
        return "I am Router {0}".format(self.router_data['RID'])

    def __del__(self):
        self.client_socket.close()

    def clientSide(self):

        server_name = 'localhost'
        message = pickle.dumps(self.router_data)

        while True:
            for dict in self.router_data['Neighbours Data']:
                #print("I am sending my LSA to router {0}".format(dict['NID']))
                self.client_socket.sendto(message, (server_name, int(dict['Port'])))
            time.sleep(UPDATE_INTERVAL)

class HeartBeatThread(Thread):

    def __init__(self, name, HB_message, neighbours, thread_lock):
        Thread.__init__(self)
        self.name = name
        self.HB_message = HB_message
        self.neighbours = neighbours
        self.thread_lock = thread_lock
        self.HB_socket = socket(AF_INET , SOCK_DGRAM)

    def run(self):
        self.broadcastHB()

    def broadcastHB(self):

        server_name = 'localhost'
        while True:
            for neighbour in self.neighbours:
                message = pickle.dumps(self.HB_message)
                self.HB_socket.sendto(message, (server_name, int(neighbour['Port'])))
            time.sleep(PERIODIC_HEART_BEAT)

    def __del__(self):
        self.HB_socket.close()

# Global graph object to represent network topology
global graph

if __name__ == "__main__":

    # Dictionary to hold data of current router
    router_information = {}

    # Open file for reading
    with open(sys.argv[1]) as f:
        data = f.read().split('\n')

    # Split the data on " "
    ID = data[0].split(" ")

    # Parse data related to the current router
    router_information['RID'] = ID[0]
    router_information['Port'] = ID[1]
    router_information['Neighbours'] = int(data[1])
    router_information['Neighbours Data'] = []
    router_information['SN'] = 0
    router_information['FLAG'] = 0

    # Temporary graph list to hold state of current network topology
    temp_graph = []

    # Grab data about all the neighbours of this router
    for line in range(2 , len(data) - 1):

        # Dict to hold data regarding each of this router's neighbours
        router_dict = {}

        neighbour = data[line].split(" ")

        router_dict['NID']  = neighbour[0]
        router_dict['Cost'] = float(neighbour[1])
        router_dict['Port'] = neighbour[2]

        # Append the dict to current routers dict of neighbours data
        router_information['Neighbours Data'].append(router_dict)

        # Package this routers data in a useful format and append to temporary graph list
        if(router_information['RID'] < router_dict['NID']):
             graph_data = [router_information['RID'], router_dict['NID'], router_dict['Cost'], router_dict['Port']]
        else:
             graph_data = [router_dict['NID'], router_information['RID'], router_dict['Cost'], router_dict['Port']]
        temp_graph.append(graph_data)

    # Copy over the data in temporary graph to global graph object (used elsewhere)
    graph = temp_graph[:]

    # Create a list to hold each thread
    threads = []

    # Create a lock to be used by all threads
    threadLock = Lock()

    # Create heart beat message to transmit
    HB_message = [{'RID' : router_information['RID']}]

    sender_thread = SendThread("SENDER", router_information, threadLock)
    receiver_thread = ReceiveThread("RECEIVER", router_information, threadLock)
    heartbeat_thread = HeartBeatThread("HEART BEAT", HB_message, router_information['Neighbours Data'], threadLock)

    # Start each thread
    sender_thread.start()
    receiver_thread.start()
    heartbeat_thread.start()

    # Append each thread to list of threads
    threads.append(sender_thread)
    threads.append(receiver_thread)
    threads.append(heartbeat_thread)

    # Call join on each tread (so that they wait)
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print(graph)

    print("Exiting Main Thread")