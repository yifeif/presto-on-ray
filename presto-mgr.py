import ray
from optparse import OptionParser
import config

#'java' '-cp' '/mnt/yic/presto-server-0.248/lib/*' '-server' '-Xmx16G' '-XX:+UseG1GC' '-XX:G1HeapRegionSize=32M' '-XX:+UseGCOverheadLimit' '-XX:+ExplicitGCInvokesConcurrent' '-XX:+HeapDumpOnOutOfMemoryError' '-XX:+ExitOnOutOfMemoryError' '-Djdk.attach.allowAttachSelf=true' '-Dnode.environment=production' '-Dnode.id=ffffffff-ffff-ffff-ffff-ffffffffffff' '-Dnode.data-dir=/mnt/yic/presto-server-0.248/var' '-Dlog.levels-file=/mnt/yic/presto-server-0.248/etc.coordinator/log.properties' '-Dconfig=/mnt/yic/presto-server-0.248/etc.coordinator/config.properties' -Dray.address=127.0.0.1:6379 'com.facebook.presto.server.PrestoOnRay'


def gen_cmd(config: config.Config):
    pass

def run_cmd(config: config.Config):
    pass

@ray.remote
class PrestorCoordinator(object):
    def __init__(self, config: config.Config):
        self._config = config

    def get_meta(self):
        pass


@ray.remote
class PrestorWorker(object):
    def __init__(self, config: config.Config):
        self._config = config

    def get_meta(self):
        pass


@ray.remote
class PrestoClusterManager(object):
    def __init__(self, config):
        self._config = config
        self._coordinators = []
        self._workers = []

    def stats(self):
        pass

    def add_worker(self):
        pass

    def del_worker(self):
        pass

    def add_coordinator(self):
        pass

    def del_coordinator(self):
        pass

    def get_coordinator_addr(self):
        pass

CLUSTER_NAME="presto"
MANAGER_NAME = CLUSTER_NAME + ".mgr"
COMMANDS = ['add_worker', 'del_worker', 'stop', 'start', 'status']


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

    if options.cluster_name is None:
        parser.error("cluster-name is required")
    if len(args) == 1 and not args[0] in set(COMMANDS):
        parser.error(f"Invalid command {args[0]}")
    if len(args) != 1:
        parser.error(f"Invalid args {args}")
    cmd = args[0]
    if cmd == start and options.presto_config is None:
        parser.error(f"config is required for cmd start")

    return (options, args[0])


def main():
    (options, cmd) = parse_options()
    ray.init(address=options.ray_addr)
    try:
        mgr = ray.get_actor(MANAGER_NAME)
    except:
        mgr = PrestoClusterManager.options(MANAGER_NAME, lifetime="detached").remote(None)
    ray.shutdown()

if __name__ == '__main__':
    main()
