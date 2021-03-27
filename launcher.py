#!/usr/bin/env python

import errno
import os
import platform
import sys
import traceback

from fcntl import flock, LOCK_EX, LOCK_NB
from optparse import OptionParser
from os import O_RDWR, O_CREAT, O_WRONLY, O_APPEND
from os.path import basename, dirname, exists, realpath
from os.path import join as pathjoin
from signal import SIGTERM, SIGKILL
from stat import S_ISLNK
from time import sleep

COMMANDS = ['run', 'start', 'stop', 'restart', 'kill', 'status']

LSB_NOT_RUNNING = 3
LSB_STATUS_UNKNOWN = 4


def find_install_path(f):
    """Find canonical parent of bin/launcher.py"""
    if basename(f) != 'launcher.py':
        raise Exception("Expected file '%s' to be 'launcher.py' not '%s'" % (f, basename(f)))
    p = realpath(dirname(f))
    if basename(p) != 'bin':
        raise Exception("Expected file '%s' directory to be 'bin' not '%s" % (f, basename(p)))
    return dirname(p)


def makedirs(p):
    """Create directory and all intermediate ones"""
    try:
        os.makedirs(p)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def load_properties(f):
    """Load key/value pairs from a file"""
    properties = {}
    for line in load_lines(f):
        k, v = line.split('=', 1)
        properties[k.strip()] = v.strip()
    return properties


def load_lines(f):
    """Load lines from a file, ignoring blank or comment lines"""
    lines = []
    for line in open(f, 'r').readlines():
        line = line.strip()
        if len(line) > 0 and not line.startswith('#'):
            lines.append(line)
    return lines


def try_lock(f):
    """Try to open an exclusive lock (inheritable) on a file"""
    try:
        flock(f, LOCK_EX | LOCK_NB)
        return True
    except (IOError, OSError):  # IOError in Python 2, OSError in Python 3.
        return False


def open_read_write(f, mode):
    """Open file in read/write mode (without truncating it)"""
    return os.fdopen(os.open(f, O_RDWR | O_CREAT, mode), 'r+')


class Process:
    def __init__(self, path):
        makedirs(dirname(path))
        self.path = path
        self.pid_file = open_read_write(path, 0o600)
        self.refresh()

    def refresh(self):
        self.locked = try_lock(self.pid_file)

    def clear_pid(self):
        assert self.locked, 'pid file not locked by us'
        self.pid_file.seek(0)
        self.pid_file.truncate()

    def write_pid(self, pid):
        self.clear_pid()
        self.pid_file.write(str(pid) + '\n')
        self.pid_file.flush()

    def alive(self):
        self.refresh()
        if self.locked:
            return False

        pid = self.read_pid()
        try:
            os.kill(pid, 0)
            return True
        except OSError as e:
            raise Exception('Signaling pid %s failed: %s' % (pid, e))

    def read_pid(self):
        assert not self.locked, 'pid file is locked by us'
        self.pid_file.seek(0)
        line = self.pid_file.readline().strip()
        if len(line) == 0:
            raise Exception("Pid file '%s' is empty" % self.path)

        try:
            pid = int(line)
        except ValueError:
            raise Exception("Pid file '%s' contains garbage: %s" % (self.path, line))
        if pid <= 0:
            raise Exception("Pid file '%s' contains an invalid pid: %s" % (self.path, pid))
        return pid


def redirect_stdin_to_devnull():
    """Redirect stdin to /dev/null"""
    fd = os.open(os.devnull, O_RDWR)
    os.dup2(fd, sys.stdin.fileno())
    os.close(fd)


def open_append(f):
    """Open a raw file descriptor in append mode"""
    # noinspection PyTypeChecker
    return os.open(f, O_WRONLY | O_APPEND | O_CREAT, 0o644)


def redirect_output(fd):
    """Redirect stdout and stderr to a file descriptor"""
    os.dup2(fd, sys.stdout.fileno())
    os.dup2(fd, sys.stderr.fileno())


def symlink_exists(p):
    """Check if symlink exists and raise if another type of file exists"""
    try:
        st = os.lstat(p)
        if not S_ISLNK(st.st_mode):
            raise Exception('Path exists and is not a symlink: %s' % p)
        return True
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    return False


def create_symlink(source, target):
    """Create a symlink, removing the target first if it is a symlink"""
    if symlink_exists(target):
        os.remove(target)
    if exists(source):
        os.symlink(source, target)


def create_app_symlinks(options):
    """
    Symlink the 'etc' and 'plugin' directory into the data directory.

    This is needed to support programs that reference 'etc/xyz' from within
    their config files: log.levels-file=etc/log.properties
    """
    if options.etc_dir != pathjoin(options.data_dir, 'etc'):
        create_symlink(
            options.etc_dir,
            pathjoin(options.data_dir, 'etc'))

    if options.install_path != options.data_dir:
        create_symlink(
            pathjoin(options.install_path, 'plugin'),
            pathjoin(options.data_dir, 'plugin'))


def build_java_execution(options, daemon):
    if not exists(options.config_path):
        raise Exception('Config file is missing: %s' % options.config_path)
    if not exists(options.jvm_config):
        raise Exception('JVM config file is missing: %s' % options.jvm_config)
    if not exists(options.launcher_config):
        raise Exception('Launcher config file is missing: %s' % options.launcher_config)
    if options.log_levels_set and not exists(options.log_levels):
        raise Exception('Log levels file is missing: %s' % options.log_levels)

    properties = options.properties.copy()

    if exists(options.log_levels):
        properties['log.levels-file'] = options.log_levels

    if daemon:
        properties['log.output-file'] = options.server_log
        properties['log.enable-console'] = 'false'

    jvm_properties = load_lines(options.jvm_config)
    launcher_properties = load_properties(options.launcher_config)

    try:
        main_class = launcher_properties['main-class']
    except KeyError:
        raise Exception("Launcher config is missing 'main-class' property")

    properties['config'] = options.config_path

    system_properties = ['-D%s=%s' % i for i in properties.items()]
    classpath = pathjoin(options.install_path, 'lib', '*')

    command = ['java', '-cp', classpath]
    command += jvm_properties + system_properties
    command += [main_class]
    command += options.arguments

    if options.verbose:
        print(' '.join(command))
        print("")

    env = os.environ.copy()

    # set process name: https://github.com/electrum/procname
    process_name = launcher_properties.get('process-name', '')
    if len(process_name) > 0:
        system = platform.system() + '-' + platform.machine()
        shim = pathjoin(options.install_path, 'bin', 'procname', system, 'libprocname.so')
        if exists(shim):
            env['LD_PRELOAD'] = (env.get('LD_PRELOAD', '') + ':' + shim).strip()
            env['PROCNAME'] = process_name

    return command, env


def run(process, options):
    if process.alive():
        print('Already running as %s' % process.read_pid())
        return

    create_app_symlinks(options)
    args, env = build_java_execution(options, False)
    args.append("-Dray.address=127.0.0.1:6379")
    makedirs(options.data_dir)
    os.chdir(options.data_dir)

    process.write_pid(os.getpid())

    redirect_stdin_to_devnull()

    os.execvpe(args[0], args, env)


def start(process, options):
    if process.alive():
        print('Already running as %s' % process.read_pid())
        return

    create_app_symlinks(options)
    args, env = build_java_execution(options, True)

    makedirs(dirname(options.launcher_log))
    log = open_append(options.launcher_log)

    makedirs(options.data_dir)
    os.chdir(options.data_dir)
    import subprocess
    import signal
    from ctypes import cdll

    PR_SET_PDEATHSIG = 1

    class PrCtlError(Exception):
        pass

    def on_parent_exit(signame):
        signum = getattr(signal, signame)
        def set_parent_exit_signal():
            result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
            if result != 0:
                raise PrCtlError('prctl failed with error code %s' % result)
        return set_parent_exit_signal

    proc = subprocess.Popen(args, env=env, stdout=log, stdin=log, preexec_fn=on_parent_exit('SIGTERM'))

    pid = proc.pid
    if pid > 0:
        process.write_pid(pid)
        print('Started as %s' % pid)
    return proc

def terminate(process, signal, message):
    if not process.alive():
        print('Not running')
        return

    pid = process.read_pid()

    while True:
        try:
            os.kill(pid, signal)
        except OSError as e:
            if e.errno != errno.ESRCH:
                raise Exception('Signaling pid %s failed: %s' % (pid, e))

        if not process.alive():
            process.clear_pid()
            break

        sleep(0.1)

    print('%s %s' % (message, pid))


def stop(process):
    terminate(process, SIGTERM, 'Stopped')


def kill(process):
    terminate(process, SIGKILL, 'Killed')


def status(process):
    if not process.alive():
       return 'Not running'
    return 'Running as %s' % process.read_pid()


def handle_command(command, options):
    process = Process(options.pid_file)
    if command == 'run':
        return run(process, options)
    elif command == 'start':
        return start(process, options)
    elif command == 'stop':
        return stop(process)
    elif command == 'restart':
        stop(process)
        start(process, options)
    elif command == 'kill':
        kill(process)
    elif command == 'status':
        return status(process)
    else:
        raise AssertionError('Unhandled command: ' + command)

class Options:
    pass
