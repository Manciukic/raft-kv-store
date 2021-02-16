# RAFT-based distributed key-value store

The project implements a distributed and strongly consistent key-value 
store using the RAFT algorithm for consensus.
The core of the RAFT algorithm will be developed using Erlang.
A Java library will be developed to communicate with the Erlang RAFT
instance.
Using this library, a simple demo key-value store will be implemented
to test the functioning of the application. This application will 
provide a REST API to clients to use the distributed store. It will 
be implemented using Servlets.

In-memory!

## Architecture
 - Erlang core (implements generic Raft FSM) - Riccardo
    - communication between nodes
 - Erlang KV server - Mirko
    - implements the KV store using the Raft core
    - provides interface (RPC get, set, getall) for "users" (Java)
 - EJB Erlang-interface - Ahmed 
    - provides KV store interface (get, set, getall)
    - talks with KV server
 - EJB Servlets (REST, admin GUI) - Mohamed
    - uses EJB Erlang-interface
    - provides REST API
    - provides admin GUI
