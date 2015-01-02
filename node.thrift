struct NodeId {
  1: string ip,
  2: i32 port,
  3: i32 key,
}

exception KeyNotFound {
  1: string key
}

service Node {
    
    // Chord protocol methods
    NodeId find_successor(1: i32 key),
    NodeId get_predecessor(),
    NodeId get_successor(),
    NodeId get_closest_preceding_finger(1: i32 key),
    oneway void notify(1: NodeId candidate_predecessor_id)

    // DHT methods
    string get(1: string key) throws (1: KeyNotFound key),
    oneway void put(1: string key, 2: string value),

    string ping()
}