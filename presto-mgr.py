#! /home/ray/anaconda3/bin/python
import ray
from optparse import OptionParser
import socket
import socketserver
import logging
import tempfile
import launcher
import shutil
import json
from pathlib import Path
logger = logging.getLogger(__name__)
INSTALL_PATH="/tmp/presto-server-0.248/"
CATALOG_PATH="/tmp/presto-server-0.248/catalog"
CLI_PATH="/tmp/presto-cli"
PRESTO_MGR="__presto_mgr"

class ConfConfig:
    def __init__(self):
        self.query_max_memory = 5  # GB
        self.query_max_memory_per_node = 1  # GB
        self.query_max_total_memory_per_node = 2  # GB


def run_cmd(cmd, tmp_dir):
    from os.path import join as pathjoin
    o = launcher.Options()
    o.install_path = INSTALL_PATH
    o.launcher_config = pathjoin(INSTALL_PATH, "bin/launcher.properties")
    o.etc_dir = pathjoin(tmp_dir, 'etc_dir')
    o.jvm_config = pathjoin(o.etc_dir, "jvm.config")
    o.config_path = pathjoin(o.etc_dir, "config.properties")
    o.log_levels = pathjoin(o.etc_dir, "log.properties")
    o.log_levels_set = True
    o.data_dir = pathjoin(o.etc_dir, "data")
    o.pid_file = pathjoin(o.data_dir, "var/run/launcher.pid")
    o.launcher_log = pathjoin(o.data_dir, 'var/log/launcher.log')
    o.server_log = pathjoin(o.data_dir, 'var/log/server.log')
    o.properties = {}
    o.arguments = []
    o.verbose = True
    node_properties = launcher.load_properties(pathjoin(o.etc_dir, "node.properties"))
    for k, v in node_properties.items():
        if k not in o.properties:
            o.properties[k] = v

    return launcher.handle_command(cmd, o)


def prep_etc(tmp_dir, config, is_coordinator, discovery_uri):
    work_dir = Path(tmp_dir)
    etc_dir = work_dir / "etc_dir"
    etc_dir.mkdir()
    config_prop_file = etc_dir / "config.properties"
    port = discovery_uri.split(':')[-1]
    with config_prop_file.open(mode='w') as config_prop:
        if is_coordinator:
            config_prop.write("coordinator=true\n")
            config_prop.write(f"http-server.http.port={port}\n")
            config_prop.write("discovery-server.enabled=true")
        else:
            config_prop.write("coordinator=false\n")
            config_prop.write("http-server.http.port=0\n")
        config_prop.write(f"discovery.uri={discovery_uri}\n")
        config_prop.write(f"query.max-memory={config.query_max_memory}GB\n")
        config_prop.write(f"query.max-memory-per-node={config.query_max_memory_per_node}GB\n")
        config_prop.write(f"query.max-total-memory-per-node={config.query_max_total_memory_per_node}GB\n")
    node_prop_file = etc_dir / "node.properties"
    with node_prop_file.open(mode='w') as node_prop:
        node_prop.write(f"node.id={tmp_dir.encode().hex()}\n")
        node_prop.write(f"node.environment=production\n")

    jvm_config_file = etc_dir / "jvm.config"
    with jvm_config_file.open(mode='w') as jvm_config:
        jvm_opts = ["-server",
         "-Xmx16G",
         "-XX:+UseG1GC",
         "-XX:G1HeapRegionSize=32M",
         "-XX:+UseGCOverheadLimit",
         "-XX:+ExplicitGCInvokesConcurrent",
         "-XX:+HeapDumpOnOutOfMemoryError",
         "-XX:+ExitOnOutOfMemoryError",
         "-Djdk.attach.allowAttachSelf=true"]
        jvm_config.write('\n'.join(jvm_opts))

    log_config_file = etc_dir / "log.properties"
    with log_config_file.open(mode='w') as log_config:
        log_config.write('\n'.join(["com.facebook.presto=INFO"]))
    (etc_dir / "catalog").symlink_to(Path(CATALOG_PATH), target_is_directory=True)

@ray.remote
class PrestoWorker(object):
    def __init__(self, discovery_uri, config):
        self._config = config
        self._work_dir = tempfile.mkdtemp()
        prep_etc(self._work_dir, config, False, discovery_uri)
        self._proc = None

    def status(self):
        return run_cmd("status", self._work_dir)

    def start(self):
        self._proc = run_cmd("start", self._work_dir)

    def stop(self):
        if self._proc:
            self._proc.kill()
            self._proc.wait()
        shutil.rmtree(self._work_dir)


@ray.remote
class PrestoCoordinator(object):
    def __init__(self, name: str, config):
        self._config = config
        self._name = name
        self._work_dir = tempfile.mkdtemp()
        self._addr = socket.gethostbyname(socket.gethostname())
        with socketserver.TCPServer(("localhost", 0), None) as s:
            self._port = s.server_address[1]
        prep_etc(self._work_dir, config, True, f'http://{self.get_address()}')
        self._count = 0
        self._workers = []
        self._proc = None

    def start(self):
        self._prof = run_cmd("start", self._work_dir)

    def stop(self):
        if self._proc:
            self._proc.kill()
            self._proc.wait()
        shutil.rmtree(self._work_dir)

    def add_worker(self):
        self._count += 1
        w = PrestoWorker.options(name=f"{self._name}.{self._count}", lifetime="detached").remote(f'http://{self.get_address()}', self._config)
        ray.wait([w.start.remote()])
        self._workers.append(w)
        return len(self._workers)

    def del_worker(self):
        w = self._workers.pop()
        r = ray.get([w.stop.remote()])
        return len(self._workers)

    def stop(self):
        ray.get([w.stop.remote() for w in self._workers])
        for w in self._workers:
            ray.kill(w)
        if self._proc:
            self._proc.kill()
            self._proc.wait()
        self._work_dir.cleanup()

    def get_address(self):
        return f"{self._addr}:{self._port}"

    def status(self):
        return f"cluster_name: {self._name} workers: {len(self._workers)}"


@ray.remote
class PrestoMetaManager(object):
    def __init__(self):
        self._clusters = {}

    def status(self):
        return ray.get([c.status.remote() for c in self._clusters.values()])

    def start(self, cluster_name, config):
        if cluster_name in self._clusters:
            return
        self._clusters[cluster_name] = PrestoCoordinator.options(name=cluster_name, lifetime="detached").remote(cluster_name, config)
        return self._clusters[cluster_name].start.remote()

    def add_worker(self, cluster_name):
        return ray.get([self._get(cluster_name).add_worker.remote()])[0]

    def del_worker(self, cluster_name):
        return ray.get([self._get(cluster_name).del_worker.remote()])[0]

    def _get(self, cluster_name):
        if cluster_name not in self._clusters:
            raise RuntimeError("No such cluster")
        return self._clusters[cluster_name]

    def stopall(self):
        ray.wait([c.stop.remote() for c in self._clusters.values()])
        stopped = self._clusters.keys()
        self._clusters = {}
        return stopped

    def get_address(self, cluster_name):
        return ray.get([self._get(cluster_name).get_address.remote()])[0]

    def stop(self, cluster_name):
        try:
            cluster = self._clusters.pop(cluster_name)
            ray.wait([cluster.stop()])
        except:
            raise RuntimeError(f"No such cluster: {cluster_name}")


COMMANDS = ['add_worker', 'del_worker', 'stop', 'start', 'status', 'coordinator', 'connect']
def create_parser():
    commands = 'Commands: ' + ', '.join(COMMANDS)
    parser = OptionParser(prog='presto-mgr', usage='usage: %prog [options] command', description=commands)
    parser.add_option('-n', '--cluster-name', action="store", type="string", dest="cluster_name")
    parser.add_option('-a', '--ray-addr', action="store", type="string", dest="ray_addr", default="auto")
    parser.add_option('-c', '--config', action="store", type="string", dest="presto_config")
    return parser


def parse_options():
    parser = create_parser()
    (options, args) = parser.parse_args()
    if len(args) == 1 and not args[0] in set(COMMANDS):
        parser.error(f"Invalid command {args[0]}")
    if len(args) != 1 and args[0] != "connect":
        parser.error(f"Only one cmds can be used. {args}")
    cmd = args[0]
    if cmd not in ("status", "stopall") and options.cluster_name is None:
        parser.error("cluster-name is required")
    if cmd == 'start' and options.presto_config is None:
        parser.error(f"config is required for cmd start")
    if options.presto_config is not None:
        with open(options.presto_config) as w:
            data = w.read()
        import json
        data = json.loads(data)
        options.presto_config = ConfConfig()
        if data.get("query_max_memory"):
            options.presto_config.query_max_memory = data.get("query_max_memory")
        if data.get("query_max_memory_per_node"):
            options.presto_config.query_max_memory_per_node = data.get("query_max_memory_per_node")
        if data.get("query_max_total_memory_per_node"):
            options.presto_config.query_max_total_memory_per_node = data.get("query_max_total_memory_per_node")
    return (options, args[0], args[1:])

def get_meta_mgr():
    try:
        meta_mgr = ray.get_actor(PRESTO_MGR)
    except ValueError:
        meta_mgr = PrestoMetaManager.options(name=PRESTO_MGR, lifetime="detached").remote()
    return meta_mgr

def main():
    (options, cmd, others) = parse_options()
    ray.init(address=options.ray_addr)

    meta_mgr = get_meta_mgr()
    if cmd == "stopall":
        clusters = ray.get([meta_mgr.remote.stopall()])[0]
        print(f"Stopped {clusters}")
        return
    elif cmd == "status":
        print("Running clusters:\n\n", '\n'.join(ray.get([meta_mgr.status.remote()])[0]))
        return

    cluster_name = options.cluster_name
    if cmd == "stop":
        ray.wait([meta_mgr.stop.remote(cluster_name)])
        return
    elif cmd == "start":
        ray.wait([meta_mgr.start.remote(cluster_name, options.presto_config)])
        return
    if cmd == "connect":
        import os
        addr = ray.get([meta_mgr.get_address.remote(cluster_name)])[0]
        args = [CLI_PATH] + others + ["--server", addr]
        os.execvpe(CLI_PATH, args, env=os.environ.copy())
        # ~/presto-cli --server 172.31.38.60:50221 --catalog mysql --scheme test
    elif cmd == "add_worker":
        print(f"Current worker num {ray.get([meta_mgr.add_worker.remote(cluster_name)])[0]}")
    elif cmd == "del_worker":
        print(f"Current worker num {ray.get([meta_mgr.del_worker.remote(cluster_name)])[0]}")
    elif cmd == "coordinator":
        print(f"{ray.get([meta_mgr.get_address.remote(cluster_name)])[0]}")

    ray.shutdown()

if __name__ == '__main__':
    '''
# config_file will contain a json
presto-mgr.py start -n presto-app -c config_file

# add worker
presto-mgr.py add_worker -n presto-app
presto-mgr.py add_worker -n presto-app
presto-mgr.py add_worker -n presto-app
presto-mgr.py add_worker -n presto-app

# check cluster status
presto-mgr.py status

# connect to presto cluster and run query
presto-mgr.py connect

... run some query ...

presto-mgr.py del_worker -n presto-app
presto-mgr.py del_worker -n presto-app

presto-mgr.py status

# stop presto app
presto-mgr.py stop -n presto-app

presto-mgr.py status
    '''
    main()
