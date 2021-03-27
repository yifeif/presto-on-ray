```
# presto-on-ray
# Start ray cluster first
> $ ray start --head
Local node IP: 172.31.38.60
2021-03-27 06:47:47,048 INFO services.py:1174 -- View the Ray dashboard at http://localhost:8265

--------------------
Ray runtime started.
--------------------

Next steps
  To connect to this Ray runtime from another node, run
    ray start --address='172.31.38.60:6379' --redis-password='5241590000000000'

  Alternatively, use the following Python code:
    import ray
    ray.init(address='auto', _redis_password='5241590000000000')

  If connection fails, check your firewall settings and network configuration.

  To terminate the Ray runtime, run
    ray stop


# Start presto cluster, with cluster name demo
> $ ./presto-mgr.py start -c ./config.default -n demo
2021-03-27 06:48:28,384 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
(pid=25420) java -cp /mnt/yic/presto-server-0.248/lib/* -server -Xmx16G -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError -Djdk.attach.allowAttachSelf=true -Dnode.id=2f746d702f746d707736753930336e61 -Dnode.environment=production -Dlog.levels-file=/tmp/tmpw6u903na/etc_dir/log.properties -Dlog.output-file=/tmp/tmpw6u903na/etc_dir/data/var/log/server.log -Dlog.enable-console=false -Dconfig=/tmp/tmpw6u903na/etc_dir/config.properties com.facebook.presto.server.PrestoServer
(pid=25420)
(pid=25420) Started as 25437


# Check the status
> $ ./presto-mgr.py status
2021-03-27 06:48:42,033 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
Running clusters:

 cluster_name: demo workers: 0

# We can see this is a cluster without any workers, let's add some worker
> $ ./presto-mgr.py add_worker -n demo

2021-03-27 06:49:23,152 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379

Current worker num 1

# Check status again
> $ ./presto-mgr.py status

2021-03-27 06:49:42,016 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
Running clusters:

 cluster_name: demo workers: 1

# Now we have one worker

> $ ./presto-mgr.py add_worker -n demo

2021-03-27 06:51:25,594 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
Current worker num 3

> $ ./presto-mgr.py del_worker -n demo
2021-03-27 06:51:41,035 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
Current worker num 2

> $ ./presto-mgr.py coordinator -n demo
2021-03-27 06:51:57,644 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
172.31.38.60:37807


# Finally connect
$ ./presto-mgr.py connect -n demo -- --catalog mysql --schema test
2021-03-27 06:52:06,089 INFO worker.py:655 -- Connecting to existing Ray cluster at address: 172.31.38.60:6379
presto:test> select * from user;
 id | name | age | income
----+------+-----+--------
  1 | Jack |  20 |   1000
(1 row)

Query 20210327_065210_00002_q3uza, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:00 [1 rows, 0B] [3 rows/s, 0B/s]

presto:test> select sum(income) from user;
 _col0
-------
  1000
(1 row)

Query 20210327_065212_00003_q3uza, FINISHED, 1 node
Splits: 18 total, 18 done (100.00%)
0:00 [1 rows, 0B] [3 rows/s, 0B/s]

presto:test>
```
