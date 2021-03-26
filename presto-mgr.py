import ray
from optparse import OptionParser

#'java' '-cp' '/mnt/yic/presto-server-0.248/lib/*' '-server' '-Xmx16G' '-XX:+UseG1GC' '-XX:G1HeapRegionSize=32M' '-XX:+UseGCOverheadLimit' '-XX:+ExplicitGCInvokesConcurrent' '-XX:+HeapDumpOnOutOfMemoryError' '-XX:+ExitOnOutOfMemoryError' '-Djdk.attach.allowAttachSelf=true' '-Dnode.environment=production' '-Dnode.id=ffffffff-ffff-ffff-ffff-ffffffffffff' '-Dnode.data-dir=/mnt/yic/presto-server-0.248/var' '-Dlog.levels-file=/mnt/yic/presto-server-0.248/etc.coordinator/log.properties' '-Dconfig=/mnt/yic/presto-server-0.248/etc.coordinator/config.properties' -Dray.address=127.0.0.1:6379 'com.facebook.presto.server.PrestoOnRay'

def make_cmd(config):
    pass

@ray.remote
class PrestorCoordinator(object):
    def __init__(self, config):
        pass

    def get_meta(self):
        pass


@ray.remote
class PrestorWorker(object):
    def __init__(self, config):
        pass

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
    parser.add_option('-f', '--cluster-name', action="store", type="string", dest="cluster_name")
    return parser


def option_verify(options):
    if options.get('cluster_name') is None:


def main():
    ray.init(address='auto')
    parser = create_parser()
    (options, args) = parser.parse_args()

    try:
        mgr = ray.get_actor(MANAGER_NAME)
    except:
        mgr = PrestoClusterManager.options(MANAGER_NAME, lifetime="detached").remote(None)

    ray.shutdown()


if __name__ == '__main__':
    main()
