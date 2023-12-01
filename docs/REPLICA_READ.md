The overarching behaviour of GetAllReplicas, is that we will attempt to get every possible replica until all replicas are returned
or the operation times out. At most NUM_REPLICAS + 1 replicas can be returned, all the replicas plus the master copy, however there
is no guarantee about the minimum number of replicas that will be returned, nor that they will include the master. However the
algorithm will return at most one document from each node.

A motivator for the re-design was to prevent duplicate reads of replicas being returned. This used to be possible, since the routing
logic uses replicaIds to find the vbuckets containing particular replicas. This can be problematic when a rebalance occurs mid
GetAllReplicas since replicaIds can be changed during a rebalance, so we could read replica 1, a rebalance occur, then get routed to
the same replica when we try to read replica 2. The new algorithm solves this issue by keeping track of which nodes we have recieved
replicas from and ignoring any duplicate reads.

In the new design we start a go routine for each possible replica, to fetch them in paralell. Something apparently unusual
is that a thread is not terminated when a replica is returned from it. This is because each thread is tied to a specific replicaId,
and as mentioned these replicaIds can be associated with docs on different nodes if a rebalance occurs. For example the thread reading
the replica with replicaId 1 could fetch the replica, then a rebalance occurs. Now imaagine the replica with replicaId 1 is now on
a node from which we have yet to read. If we had terminated the replicaId 1 thread afer the first successful read we would have
no way of reading the moved replica, so we keep every thread alive and polling it's associated replica to see if it has moved.

Errors can be returned by a call to GetAllReplicas in two ways. Firstly GetAllReplicas can directly return an error when some
state has occured under which none of the documents are able to be fetched, e.g the collection ID cannot be found. Then there are
errors that can be streamed back from each of the individual replica reads, this allows the user to decide how to handle the error
given their use case for GetAllReplicas. If they are using it to get any replica, they may ignore and continue, however if they wanted
a quorum then a single error may be fatal, but we want the user to be able to make this decision.