A DAG execution module in golang.

Runs a DAG of actions of type [Value] -> Value in parallel, using channels to pass the outputs of each node along the edges. Errors are propagated through to a finish node, and returned by Graph.Run.
