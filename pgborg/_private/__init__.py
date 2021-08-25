# -*- coding: utf-8 -*-

# Copyright 2020-2021 Kungliga Tekniska högskolan

# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import print_function, unicode_literals

# This is https://github.com/systemd/python-systemd a.k.a.
# https://pypi.org/project/systemd-python/
# (not https://pypi.org/project/systemd/)
from systemd.journal import JournalHandler
from systemd.daemon import notify

import os
import logging
logging.basicConfig(level='DEBUG', format='%(message)s', **({'handlers': [JournalHandler()]} if 'JOURNAL_STREAM' in os.environ else {}))

import argparse
import configparser
import datetime
import itertools
import json
import pathlib
import re
import shlex
import signal
import socket
import sys

from pwd import getpwnam
from grp import getgrnam
from subprocess import check_output, check_call, DEVNULL, CalledProcessError
from tempfile import TemporaryDirectory, mkdtemp
from time import sleep, clock_gettime, CLOCK_MONOTONIC


# TODO split the sections below into separate modules outside _private, etc cleanup


## PostgreSQL across-fork-and-setuid proxies

from multiprocessing import set_start_method, Process, Pipe, Queue
import psycopg2

def ping_server(pipe_connection):
    pipe_connection.poll(10)
    assert(pipe_connection.recv() == 'ping')
    pipe_connection.send('pong')

def mp_init():
    set_start_method('forkserver')
    parent_pipe_connection, child_pipe_connection = Pipe()
    ping_p = Process(target=ping_server, args=(child_pipe_connection,))
    ping_p.start()
    parent_pipe_connection.send('ping')
    parent_pipe_connection.poll(10)
    assert(parent_pipe_connection.recv() == 'pong')
    ping_p.join()

def pop_final(queue, timeout=10):
    result = queue.get(True, timeout)
    try:
        while queue.get(False):
            pass
    except:
        pass
    queue.close()
    return result

def clear_pipe(p):
    while p.poll():
        p.recv()
    p.close()

def postgresql_list_databases_process(result, success, connstring, uid, gid, groups):
    error = None
    datnames = None
    try:
        if gid is not None:
            os.setgid(gid)
        if groups is not None:
            os.setgroups(groups)
        if uid is not None:
            os.setuid(uid)
        with psycopg2.connect(connstring) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT datname FROM pg_database WHERE datistemplate = false;")
                datnames = [datname for (datname,) in cur.fetchall()]
    except Exception as e:
        error = e
    finally:
        result.put(datnames)
        result.close()
        success.put(error)
        success.close()

class PostgreSQLListDatabasesContext():
    def __init__(self, connstring, uid=None, gid=None, groups=None):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        result = Queue()
        success = Queue()
        self._logger.info("Listing databases...")
        self.process = Process(target=postgresql_list_databases_process, args=(result, success, connstring, uid, gid, groups))
        self.process.start()
        try:
            self._logger.info("Waiting for result...")
            res = pop_final(result, timeout=60)
            assert(res is not None)
            self._logger.info("Got result.")
            self.names = res
        finally:
            self._logger.info("Cleaning up...")
            try:
                error = pop_final(success)
                if error is not None:
                    raise error
            finally:
                self.process.join()
                self._logger.info("Done.")

def postgresql_manage_backup_process(result, started, stop, success, connstring, baklabel, uid, gid, groups):
    error = None
    gotlabel = None
    try:
        if gid is not None:
            os.setgid(gid)
        if groups is not None:
            os.setgroups(groups)
        if uid is not None:
            os.setuid(uid)
        with psycopg2.connect(connstring) as conn:
            started_res = None
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_start_backup(%s, false, false);", (baklabel,))
                    res = cur.fetchone()
                    cur.execute("SELECT pg_switch_wal();")
                    cur.fetchone()
                    started_res = 'started-backup'
            finally:
                started.put(started_res)
                started.close()
                try:
                    # Wait for stop command
                    assert(pop_final(stop, timeout=120) == 'stop-backup')
                finally:
                    # This is run whether we got stop or not
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM pg_stop_backup(false, true);")
                        res = cur.fetchone()
                        gotlabel = res[1]
    except Exception as e:
        error = e
    finally:
        result.put(gotlabel)
        result.close()
        success.put(error)
        success.close()

class PostgreSQLBackupContext():
    def __init__(self, connstring, baklabel, uid=None, gid=None, groups=None):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        parent_pipe_connection, child_pipe_connection = Pipe()
        self.result_queue = Queue()
        self.started_queue = Queue()
        self.stop_queue = Queue()
        self.success = Queue()
        self.process = Process(target=postgresql_manage_backup_process, args=(self.result_queue, self.started_queue, self.stop_queue, self.success, connstring, baklabel, uid, gid, groups))
        self.backup_label = None

    def __enter__(self):
        return self

    def start(self):
        self.process.start()
        assert(pop_final(self.started_queue, timeout=120) == 'started-backup')

    def stop(self):
        self.stop_queue.put('stop-backup')
        self.stop_queue.close()
        self.stop_queue = None
        self.backup_label = pop_final(self.result_queue, timeout=60)
        assert(self.backup_label is not None)
        return self.backup_label

    def __exit__(self, type, value, traceback):
        if self.stop_queue is not None:
            self.stop_queue.put('stop-backup')
            self.stop_queue.close()
        try:
            error = pop_final(self.success)
            if error is not None:
                raise error
        finally:
            self.process.join()
            self._logger.info("Done.")
            self.process = None


## Backup and restore

def esc(command):
    return " ".join([shlex.quote(e) for e in command])

class PostgreSQLRestoreProcess():
    def read_args(self, args):
        service_filter_args = {}
        if args.instance:
            service_filter_args['instance'] = args.instance
        if args.pgversion:
            service_filter_args['pgversion'] = args.pgversion

        system = SystemServices()
        services = system.get_restore_postgresql_services(**service_filter_args)
        if len(services) == 0:
            raise Exception("No matching PostgreSQL service found, try specifying --instance.")
        if len(services) > 1:
            raise Exception("Multiple matching PostgreSQL service found, specify both --instance and --pgversion if needed.")
        self.service = services[0]

        # Verify config existance.
        pgdata = pathlib.Path(self.service.environment['PGDATA'])
        conf = PostgreSQLConfig(pgdata / "postgresql.conf")

        env = backup_conf.get_service_environment(self.service)
        self.borg = BorgClient(env=env)

        self.args = args

    def main(self):
        self.args.func(self.args)

class PostgreSQLDumpRestoreProcess(PostgreSQLRestoreProcess):
    def __init__(self):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        backup_conf = PerInstanceConfigFile("pgdump")
        backup_conf.apply_config()

        def cmd_extract(args):
            archive_manager = PostgreSQLDumpArchiveManager(self.borg, fqdn=args.fqdn, instance=self.service.instance, pgversion=self.service.pgversion)
            if args.time:
                dt = datetime.datetime.fromisoformat(args.time).astimezone()
                timestamp = dt.strftime("%Y%m%d")
                backup = archive_manager.find_backup_before(timestamp, args.dbname)
            else:
                backup = archive_manager.find_latest_backup(args.dbname)

            source_command = backup.get_stream_command(f"{args.dbname}.pgsql")
            if self.sink_command:
                sink_command = self.sink_command
                if self.service.user:
                    sink_command = ["/bin/su", "-", "-c", esc(sink_command), self.service.user]
                pipe_commands = [source_command, sink_command]
                pipe_cmdlines = [esc(command) for command in pipe_commands]
                pipe_cmdline = "set -e; set -o pipefail; " + " | ".join(pipe_cmdlines)
                self._logger.debug(f"Running shell command line: {pipe_cmdline}")
                check_call(pipe_cmdline, shell=True, stdin=DEVNULL)
            else:
                self._logger.debug(f"Running command: {esc(source_command)}")
                check_call(source_command, stdin=DEVNULL)


        parser = argparse.ArgumentParser(description='PostgreSQL dump restore utility')
        subparsers = parser.add_subparsers(help='Commands', dest='command')

        extract_parser = subparsers.add_parser('extract', help='extract command')
        extract_parser.add_argument('--fqdn', action='store', help='fqdn of backup to read')
        extract_parser.add_argument('--instance', action='store', help='instance name of backup to read')
        extract_parser.add_argument('--pgversion', action='store', help='version of postgresql')
        extract_parser.add_argument('--time', action='store', help='extract point')
        extract_parser.add_argument('dbname', action='store', help='database name')
        extract_parser.add_argument('command', nargs=argparse.REMAINDER, help='command to pipe into (unless stdout)')
        extract_parser.set_defaults(func=cmd_extract)

        args = parser.parse_args()

        if not hasattr(args, 'command') or args.command == None:
            parser.print_usage(sys.stderr)
            sys.exit(1)

        sink_command = args.command
        while sink_command is not None and len(sink_command) > 0 and sink_command[0] == '--':
            sink_command = sink_command[1:]
        self.sink_command = sink_command if len(sink_command) > 0 else None

        self.read_args(args)

class PostgreSQLDumpProcess():
    def __init__(self):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        self._notifystatus = NotifyStatus()

        self._notifystatus.set_status("OK init")

        try:
            backup_conf = PerInstanceConfigFile("pgdump")
            backup_conf.apply_config()

            self._notifystatus.set_status("OK starting")

            def callback(*args, **kwargs):
                self._logger.info("Stopping...")
            self._stop = StopHandler(callback)

            self._databases = PostgreSQLServerBackupProcess(backup_conf, PostgreSQLInstanceDumpBackupProcessGenerator())
        except Exception as e:
            self._notifystatus.set_status(f"CRITICAL {e}")
            raise e

    def main(self):
        try:
            self._logger.info("Starting backups...")

            self._databases.begin_backup_processes()
            self._databases.expire_backups()

            self._logger.info("Backup running.")
            sleeptime = CountDown(10)
            while True:
                self._databases.maybe_expire_backups()

                if sleeptime.reached_liftoff:
                    t = StopWatch()
                    self._databases.do_backups()
                    sleeptime.reset(t=min(3600,max(3600*24,t.total*10)))

                self._notifystatus.set_status("OK running")
                self._notifystatus.set_ready()
                self._stop.sleep(60)
                if self._stop.stop:
                    break
        except Exception as e:
            self._notifystatus.set_status(f"CRITICAL {e}")
            raise e

class PostgreSQLContinuousArchiveRestoreProcess(PostgreSQLRestoreProcess):
    def __init__(self):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        backup_conf = PerInstanceConfigFile("pgca")
        backup_conf.apply_config()

        def cmd_info(args):
            if args.time:
                timestamp = None
                if args.time.startswith("restorepoint-"):
                    timestamp = args.time.split("-")[1]
                    recovery_opts['recovery_target_name'] = args.time
                else:
                    dt = datetime.datetime.fromisoformat(args.time).astimezone()
                    timestamp = dt.strftime("%Y%m%d")
                    recovery_opts['recovery_target_time'] = dt.isoformat()
                base_backup = PostgreSQLContinuousArchiveStorageManager(self.borg, fqdn=args.fqdn, instance=self.service.instance, pgversion=self.service.pgversion).find_backup_before(timestamp)
            else:
                base_backup = PostgreSQLContinuousArchiveStorageManager(self.borg, fqdn=args.fqdn, instance=self.service.instance, pgversion=self.service.pgversion).find_latest_backup()

            pgdata = pathlib.Path(self.service.environment['PGDATA'])
            print(f"Would restore {base_backup} to {pgdata}")

        def cmd_restore(args):
            recovery_opts = {}

            if args.time:
                timestamp = None
                if args.time.startswith("restorepoint-"):
                    timestamp = args.time.split("-")[1]
                    recovery_opts['recovery_target_name'] = args.time
                else:
                    dt = datetime.datetime.fromisoformat(args.time).astimezone()
                    timestamp = dt.strftime("%Y%m%d")
                    recovery_opts['recovery_target_time'] = dt.isoformat()
                base_backup = PostgreSQLContinuousArchiveStorageManager(self.borg, fqdn=args.fqdn, instance=self.service.instance, pgversion=self.service.pgversion).find_backup_before(timestamp)
            else:
                base_backup = PostgreSQLContinuousArchiveStorageManager(self.borg, fqdn=args.fqdn, instance=self.service.instance, pgversion=self.service.pgversion).find_latest_backup()

            pgdata = pathlib.Path(self.service.environment['PGDATA'])
            base_backup.restore_to(pgdata, recovery_opts)

        parser = argparse.ArgumentParser(description='PostgreSQL continuous archive restore utility')
        subparsers = parser.add_subparsers(help='Commands', dest='command')

        info_parser = subparsers.add_parser('info', help='Info command')
        info_parser.add_argument('--fqdn', action='store', help='fqdn of backup to read')
        info_parser.add_argument('--instance', action='store', help='instance name of backup to read')
        info_parser.add_argument('--time', action='store', help='info point')
        info_parser.set_defaults(func=cmd_info)

        restore_parser = subparsers.add_parser('restore', help='Restore command')
        restore_parser.add_argument('--fqdn', action='store', help='fqdn of backup to read')
        restore_parser.add_argument('--instance', action='store', help='instance name of backup to read')
        extract_parser.add_argument('--pgversion', action='store', help='version of postgresql')
        restore_parser.add_argument('--time', action='store', help='restore point')
        restore_parser.set_defaults(func=cmd_restore)

        args = parser.parse_args()

        if not hasattr(args, 'command') or args.command == None:
            parser.print_usage(sys.stderr)
            sys.exit(1)

        self.read_args(args)

class PostgreSQLContinuousArchivingProcess():
    def __init__(self):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        self._notifystatus = NotifyStatus()

        self._notifystatus.set_status("OK init")

        try:
            backup_conf = PerInstanceConfigFile("pgca")
            backup_conf.apply_config()

            self._notifystatus.set_status("OK starting")

            def callback(*args, **kwargs):
                self._logger.info("Stopping...")
            self._stop = StopHandler(callback)

            self._databases = PostgreSQLServerBackupProcess(backup_conf, PostgreSQLInstanceContinuousArchiveBackupProcessGenerator())
        except Exception as e:
            self._notifystatus.set_status(f"CRITICAL {e}")
            raise e

    def main(self):
        self._logger.info("Starting backups...")

        def cleanup():
            self._logger.info("Cleaning up...")
            self._databases.end_backup_processes()
            self._logger.info("Cleanup done.")

        try:
            self._databases.begin_backup_processes()
            self._databases.expire_backups()

            self._logger.info("Backup running.")
            while True:
                self._databases.maybe_expire_backups()

                # TODO use inotify
                self._databases.do_backups()

                self._notifystatus.set_status("OK running")
                self._notifystatus.set_ready()

                self._stop.sleep(10)
                if self._stop.stop:
                    break
        except Exception as e:
            self._notifystatus.set_status(f"CRITICAL {e}")
            raise e
        finally:
            cleanup()


## PostgreSQL utils

class CaseConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr):
        return optionstr

class PerInstanceConfigFile():
    def __init__(self, appname):
        self._logger = None
        self.appname = appname
        self.conf = CaseConfigParser()
        conf_env_var_name = f"{appname}_ini".upper()
        conffile = pathlib.Path(os.environ[conf_env_var_name]) if conf_env_var_name in os.environ else pathlib.Path("/etc") / f"{appname}.ini"
        if conffile.exists():
            self.conf.read(conffile)

    def apply_config(self):
        logging.getLogger(__name__).setLevel(self.conf.get('logging', 'level', fallback='INFO'))
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

    def get_service_environment(self, service):
        return self.get_merged_environment(self.conf_sections_for_service(service))

    def conf_sections_for_service(self, service):
        i = service.instance if (service.instance) else "default"
        v = service.pgversion
        return self.conf_sections(i, v)

    def conf_sections(self, i, v):
        return ["environment", f"env-{i}", f"env-{i}-{v}"]

    def get_merged_environment(self, conf_sections):
        env = {'XDG_CACHE_HOME': f"/var/cache/{self.appname}"}
        for conf_section in conf_sections:
            if conf_section in self.conf:
                env.update(self.conf[conf_section])
        return env

class PostgreSQLInstanceDumpBackupProcessGenerator():
    def generate(self, service, borg):
        return PostgreSQLInstanceDumpBackupProcess(service, borg)

class PostgreSQLInstanceContinuousArchiveBackupProcessGenerator():
    def generate(self, service, borg):
        return PostgreSQLInstanceContinuousArchiveBackupProcess(service, borg)

class PostgreSQLServerBackupProcess():
    def __init__(self, backup_conf, backup_process_generator):
        self._last_expire = None
        self.backup_process_generator = backup_process_generator
        databases = set()

        system = SystemServices()
        services = system.get_backup_postgresql_services()

        for service in services:
            if service.is_enabled() or service.is_active():
                env = backup_conf.get_service_environment(service)
                borg = BorgClient(env=env)
                db = self.backup_process_generator.generate(service, borg)
                databases.add(db)

        if len(databases) == 0:
            raise Exception("No PostgreSQL instances to back up.")

        self.databases = databases

    def begin_backup_processes(self):
        for db in self.databases:
            db.begin_backup_processes()

    def maybe_expire_backups(self):
        if self._last_expire is None or clock_gettime(CLOCK_MONOTONIC) - self._last_expire > 36000:
            self.expire_backups()

    def expire_backups(self):
        self._last_expire = clock_gettime(CLOCK_MONOTONIC)
        for db in self.databases:
            db.expire_backups()

    def do_backups(self):
        for db in self.databases:
            db.do_backups()

    def end_backup_processes(self):
        def end_dbs(dbs):
            if len(dbs) == 0:
                return
            try:
                dbs[0].end_backup_processes()
            finally:
                end_dbs(dbs[1:])
        end_dbs(list(self.databases))

class PostgreSQLConfig():
    def __init__(self, filename):
        conf_line = re.compile("^([a-z][a-z_]*[a-z])\s*=\s*(.+)\s*(#.*|)$")
        conf = {}
        with open(filename, "r") as f:
            for l in f:
                m = conf_line.match(l.strip())
                if m:
                    conf[m.group(1)] = m.group(2)

        self.values = conf
        if 'port' in conf:
            self.port = int(conf['port'].split(" ")[0].split("#")[0])
        else:
            self.port = 5432

class PostgreSQLInstanceDumpBackupProcess():
    def __init__(self, service, borg):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

        self.service = service

        pgdata = pathlib.Path(service.environment['PGDATA'])
        conf = PostgreSQLConfig(pgdata / "postgresql.conf")
        self.port = conf.port
        self.connstring = f"port={self.port}"

        self.archive_manager = PostgreSQLDumpArchiveManager(borg, instance=service.instance, pgversion=self.service.pgversion)

    def scan_datnames(self):
        datname_lister = PostgreSQLListDatabasesContext(self.connstring, uid=self.service.user_uid, gid=self.service.user_gid, groups=[self.service.user_gid])
        self.datnames = datname_lister.names
        assert(len(self.datnames) > 0)

    def begin_backup_processes(self):
        self.scan_datnames()

    def do_backups(self):
        self.scan_datnames()
        for datname in self.datnames:
            self._logger.info(f"Backing up: {datname}")
            with PostgreSQLDumpTime(self, datname, self.archive_manager) as dump:
                sink_command = dump.get_create_tmp_pgdump_cmd()

                # Disable compression to allow Borg to deduplicate.
                source_command = ["pg_dump", "-Fc", "--compress=0", f"--port={self.port}", datname]
                if self.service.user:
                    source_command = ["/bin/su", "-", "-c", esc(source_command), self.service.user]

                pipe_commands = [source_command, sink_command]
                pipe_cmdlines = [esc(command) for command in pipe_commands]
                pipe_cmdline = "set -e; set -o pipefail; " + " | ".join(pipe_cmdlines)
                self._logger.debug(f"Running shell command line: {pipe_cmdline}")
                check_call(pipe_cmdline, shell=True, stdin=DEVNULL)

                dump.commit()
            self._logger.info(f"Done.")

    def expire_backups(self):
        for datname in self.datnames:
            self.archive_manager.expire_backups(datname)

class PostgreSQLDumpTime():
    def __init__(self, db, datname, archive_manager):
        self.dbinstancebackupprocess = db
        self.datname = datname
        self.archive_manager = archive_manager
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.success = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self.success:
            self.archive_manager.delete_tmp_pgdump(self.timestamp, self.datname)

    def get_create_tmp_pgdump_cmd(self):
        return self.archive_manager.get_create_tmp_pgdump_cmd(self.timestamp, self.datname)

    def commit(self):
        self.archive_manager.rename_tmp_pgdump(self.timestamp, self.datname)
        self.success = True

class PostgreSQLInstanceContinuousArchiveBackupProcess():
    def __init__(self, service, borg):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.pgdata = pathlib.Path(service.environment['PGDATA'])

        self.service = service

        self.conf = PostgreSQLConfig(self.pgdata / "postgresql.conf")
        self.port = self.conf.port
        self.borg = borg

        self.archive_manager = PostgreSQLContinuousArchiveStorageManager(self.borg, instance=service.instance, pgversion=service.pgversion)

    def get_archive_cycle(self, base_timestamp):
        return PostgreSQLContinuousArchiveCycleArchiver(self.borg, self.archive_manager, base_timestamp)

    def get_superuser_connstring(self):
        return f"port={self.port}"

    def begin_backup_processes(self):
        self._logger.info("Cleaning up old backup processes...")

        # Find WAL directories which exist now.
        self.current_backup = PostgreSQLInstanceOldContinuousArchiveCycleCleanup(self)
        self.current_backup.begin()

        self.do_backups()

    def do_backups(self):
        if self.current_backup.is_expired:
            # Begin with a new base backup
            next_backup = PostgreSQLInstanceContinuousArchiveCycle(self, self.borg)
            next_backup.begin()

            # Ensure all WALs for old base, until after the new base, are archived.
            self.current_backup.do_backup_and_end()
            self.current_backup = None

            self.current_backup = next_backup

        self.current_backup.do_backup()

    def end_backup_processes(self):
        # We do not run do_backup_and_end() because we want the last
        # WAL dir to be kept until this service is started again.
        self.current_backup.do_backup()

    def expire_backups(self):
        self.archive_manager.expire_backups()

class PostgreSQLContinuousArchiveCycleArchiver():
    def __init__(self, borg, archive_manager, base_timestamp):
        self.borg = borg
        self.archive_manager = archive_manager
        self.base_timestamp = base_timestamp
        self.count = 0
        self.compressed_base_size = 0
        self.total_compressed_wal_size = 0

    @property
    def wal_heavy(self):
        return self.compressed_base_size < self.total_compressed_wal_size

    def get_base_pgdata_name(self):
        return self.archive_manager.get_base_pgdata_name(self.base_timestamp)

    def get_wal_name(self, archive_serial):
        return self.archive_manager.get_wal_name(self.base_timestamp, archive_serial)

    def archive_base(self, path):
        arch = self.borg.archive(self.get_base_pgdata_name(), path, ".", exclude=["pg_wal/*", "postmaster.pid", "postmaster.opts", "pg_replslot/*", "pg_dynshmem/*", "pg_notify/*", "pg_serial/*", "pg_snapshots/*", "pg_stat_tmp/*", "pg_subtrans/*", "pgsql_tmp*"])
        self.compressed_base_size = arch.compressed_size

    def archive_wals(self, path):
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.count += 1
        arch = self.borg.archive(self.get_wal_name(f"{timestamp}-{self.count}"), path, ".")
        self.total_compressed_wal_size += arch.compressed_size

class PostgreSQLDumpArchiveManager():
    def __init__(self, borg, fqdn=None, instance=None, pgversion=None):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.borg = borg
        if fqdn is None:
            fqdn = socket.getfqdn()
        if instance is None:
            instance = "default"
        if pgversion == None or pgversion == "default":
            pgversion=""
        self.pgsql_prefix = f"{fqdn}-{instance}-pgsql{pgversion}-"

    def find_latest_backup(self, datname):
        prefix = f"{self.pgsql_prefix}pgdump-{datname}-"
        self._logger.info(f"Listing archives with prefix: {prefix}")
        archives = list(self.borg.archives(prefix=prefix))
        archives.sort()
        return archives[-1]

    def get_create_tmp_pgdump_cmd(self, timestamp, datname):
        dumpname = self.get_dump_name(timestamp, datname)
        return self.borg.get_create_from_stdin_cmd(f"{dumpname}.tmp", stdin_name=f"{datname}.pgsql")

    def delete_tmp_pgdump(self, timestamp, datname):
        dumpname = self.get_dump_name(timestamp, datname)
        self.borg.delete(f"{dumpname}.tmp")

    def rename_tmp_pgdump(self, timestamp, datname):
        dumpname = self.get_dump_name(timestamp, datname)
        self.borg.rename(f"{dumpname}.tmp", dumpname)

    def get_dump_name(self, timestamp, datname):
        return f"{self.pgsql_prefix}pgdump-{datname}-{timestamp}"

    def escape_datname(self, s):
        if s.startswith("_") or "-" in s:
            escape_character = [c for c in ["X","Y","Z"] if c not in s][0]
            def escape_with(e, plain):
                if len(plain) == 0:
                    return ""
                if plain[0] in [e, "_", "-"]:
                    escaped_char = f"{e}{ord(plain[0])}{e}"
                    rest = escape_with(e, plain[1:])
                    return f"{escaped_char}{rest}"
            return f"_{escape_character}_{escaped_string}"
        return s

    def expire_backups(self, datname):
        borg_datname = self.escape_datname(datname)
        self.borg.expire_prefix(f"{self.pgsql_prefix}pgdump-{borg_datname}-")

class PostgreSQLContinuousArchiveStorageManager():
    def __init__(self, borg, fqdn=None, instance=None, pgversion=None):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.borg = borg
        if fqdn is None:
            fqdn = socket.getfqdn()
        if instance is None:
            instance = "default"
        if pgversion == None or pgversion == "default":
            pgversion=""
        self.pgsql_prefix = f"{fqdn}-{instance}-pgsql{pgversion}-"

    def get_base_pgdata_name(self, base_timestamp):
        return f"{self.pgsql_prefix}base-{base_timestamp}-snapshot-pgdata"

    def get_wal_name(self, base_timestamp, archive_serial):
        return f"{self.pgsql_prefix}base-{base_timestamp}-wals-{archive_serial}"

    def find_latest_backup(self):
        self.scan_backups()
        base_backups = list(self.backups.values())
        base_backups.sort()
        return base_backups[-1]

    def find_backup_before(self, timestamp):
        self.scan_backups()
        base_backups = [backup for backup in self.backups.values() if backup.base_timestamp <= timestamp]
        base_backups.sort()
        return base_backups[-1]

    def expire_backups(self):
        self.scan_backups()
        for wal in self.orphans_wals:
            self._logger.info(f"Deleting archive: {wal}")
            wal.delete()
        base_backups = list(self.backups.values())
        base_backups.sort()
        delete_backups = base_backups[0:-3]
        for backup in delete_backups:
            if datetime.datetime.strptime(backup.base_timestamp, "%Y%m%d%H%M%S") < datetime.datetime.now() - datetime.timedelta(days=90):
                self._logger.info(f"Deleting backups: {backup}")
                backup.delete()

    def scan_backups(self):
        pgsql_base_prefix = f"{self.pgsql_prefix}base-"
        self._logger.info(f"Scanning backups with prefix {pgsql_base_prefix}")
        base_backups = {}
        wals = set()
        needed_wals = set()
        for backup_archive in self.borg.archives(prefix=pgsql_base_prefix):
            assert(backup_archive.name.startswith(pgsql_base_prefix))
            if "-snapshot-pgdata" in backup_archive.name:
                base_backup = PostgreSQLBaseArchive(pgsql_base_prefix, backup_archive)
                base_backups[base_backup.base_timestamp] = base_backup
            elif "-wals-" in backup_archive.name:
                wal_archive = PostgreSQLWALArchive(pgsql_base_prefix, backup_archive)
                wals.add(wal_archive)
            else:
                self._logger.warning(f"Unknown archive {backup_archive.name}")
        for wal in wals:
            if wal.base_timestamp in base_backups:
                base_backups[wal.base_timestamp].add_archives(wal)
                needed_wals.add(wal)
            else:
                self._logger.warning(f"No base backup found for {wal} with base timestamp {wal.base_timestamp}")
        for base_timestamp,base_backup in base_backups.items():
            if len(base_backup.wal_archives) == 0:
                self._logger.warning(f"Base backup {base_backup.base_backup_archive.name} has no WALs")
        self.backups = base_backups
        self.orphans_wals = wals - needed_wals

class PostgreSQLBaseArchive():
    def __init__(self, pgsql_base_prefix, backup_archive):
        assert(backup_archive.name.startswith(pgsql_base_prefix))
        self.base_timestamp = backup_archive.name[len(pgsql_base_prefix):].split("-")[0]
        self.base_backup_archive = backup_archive
        self.wal_prefix = f"{pgsql_base_prefix}{self.base_timestamp}-"
        self.wal_archives = set()

    def add_archives(self, wal_archive):
        assert(wal_archive.name.startswith(self.wal_prefix))
        self.wal_archives.add(wal_archive)

    def __lt__(self, other):
        return self.base_backup_archive.name < other.base_backup_archive.name

    def __eq__(self, other):
        return self.base_backup_archive.name == other.base_backup_archive.name

    def __str__(self):
        return self.base_backup_archive.name

    def restore_to(self, pgdata, recovery_opts):
        self.extract_base_to(pgdata)
        statinfo = os.lstat(pgdata)

        waltmp = mkdtemp(prefix=f"wal-{self.base_backup_archive.name}-", dir=pgdata.parent)
        walxdir = pathlib.Path(waltmp) / "wals"
        self.extract_wals_to(walxdir)
        os.chown(walxdir, statinfo.st_uid, statinfo.st_gid)
        os.chown(waltmp, statinfo.st_uid, statinfo.st_gid)

        p = pgdata / "recovery.conf"
        with open(p, "w") as f:
            for (key,value) in recovery_opts.items():
                print(f"{key} = {value}", file=f)
            print(f"restore_command = 'cp {walxdir.as_posix()}/%f \"%p\"'", file=f)
        statinfo = os.lstat(pgdata)
        os.chown(p, statinfo.st_uid, statinfo.st_gid)

    def extract_base_to(self, dest_dir):
        if dest_dir.exists():
            raise Exception(f"Cannot restore over existing {dest_dir.as_posix()}")
        with TemporaryDirectory(dir = dest_dir.parent, prefix = f"{dest_dir.name}.restore.") as td:
            d = pathlib.Path(td) / "tmp"
            d.mkdir()
            self.base_backup_archive.extract(cwd=d)
            d.rename(dest_dir)

    def extract_wals_to(self, dest_dir):
        with TemporaryDirectory(dir=dest_dir.parent) as td:
            d = pathlib.Path(td) / "tmp"
            d.mkdir()
            for wal in self.wal_archives:
                wal.extract(cwd=d)
            d.rename(dest_dir)

    def delete(self):
        self.base_backup_archive.delete()
        for wal in self.wal_archives:
            wal.delete()

class PostgreSQLWALArchive():
    def __init__(self, pgsql_base_prefix, backup_archive):
        assert(backup_archive.name.startswith(pgsql_base_prefix))
        self.backup_archive = backup_archive
        self.name = self.backup_archive.name
        self.base_timestamp = self.backup_archive.name[len(pgsql_base_prefix):].split("-")[0]

    def extract(self, cwd=None):
        self.backup_archive.extract(cwd=cwd)

    def delete(self):
        self.backup_archive.delete()

    def __str__(self):
        return self.backup_archive.name

# Find all WAL dirs and archive those, then delete them.
class PostgreSQLInstanceOldContinuousArchiveCycleCleanup():
    def __init__(self, db):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.dbinstancebackupprocess = db
        self.is_expired = True

    def begin(self):
        m = re.compile(f"^{self.dbinstancebackupprocess.pgdata.name}.wals.(.*)$")
        self.walspools = set()
        for dirent in os.scandir(path=self.dbinstancebackupprocess.pgdata.parent):
            if dirent.is_dir(follow_symlinks=False):
                match = m.match(dirent.name)
                if match:
                    base_timestamp = match.group(1)
                    self._logger.info(f"Old WAL spool {dirent.name}")
                    archive_cycle = self.dbinstancebackupprocess.get_archive_cycle(base_timestamp)
                    self.walspools.add(PostgreSQLWalSpool(archive_cycle, dirent))

    def do_backup(self):
        for walspool in self.walspools:
            walspool.do_spool()

    def do_backup_and_end(self):
        for walspool in self.walspools:
            walspool.do_backup_and_end()

class PostgreSQLWalSpool():
    def __init__(self, archive_cycle, pathlike):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.path = pathlike
        self.archive_cycle = archive_cycle

    def get_files(self):
        return [dirent for dirent in os.scandir(path=self.path) if dirent.is_file(follow_symlinks=False) and "tmp" not in dirent.name]

    def wait_for_archive(self, timeout=None):
        t = clock_gettime(CLOCK_MONOTONIC)
        while timeout is None or clock_gettime(CLOCK_MONOTONIC) - t < timeout:
            existing = self.get_files()
            if len(existing) > 0:
                return
            sleep(1)
        raise Exception(f"Time out waiting for archives in {self.path.as_posix()}")

    def do_spool(self):
        existing = self.get_files()
        if len(existing) > 0:
            # Back up all files in this dir.
            arch = self.archive_cycle.archive_wals(self.path)
        for f in existing:
            p = pathlib.Path(f).absolute()
            try:
                p.unlink()
            except:
                self._logger.warning(f"Could not delete: {p.as_posix()}")

    # When this is called, there should already by another spool picking up new archives.
    def do_backup_and_end(self):
        self._logger.info(f"Cleaning up {pathlib.Path(self.path).as_posix()}...")
        self.do_spool()
        try:
            pathlib.Path(self.path).rmdir()
        except:
            p = pathlib.Path(self.path).absolute()
            self._logger.warning(f"Could not delete dir: {p.as_posix()}")
            try:
                #os.rename(self.path, p.parent / f"deleteme-{self.path.name}")
                pass
            except:
                self._logger.warning(f"Could not rename dir: {p.as_posix()}")

class PostgreSQLInstanceContinuousArchiveCycle():
    def __init__(self, db, borg):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.base_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.dbinstancebackupprocess = db
        self.archive_cycle = self.dbinstancebackupprocess.get_archive_cycle(self.base_timestamp)
        self.is_expired = False

    def begin(self):
        self._logger.info("Preparing for backup.")

        # Check parameters
        if 'wal_level' in self.dbinstancebackupprocess.conf.values and self.dbinstancebackupprocess.conf.values['wal_level'] not in ["archive", "hot_standby", "replica", "logical"]:
            raise Exception("Change wal_level to either 'replica' (the default) or 'logical' first.")
        archive_command_string = 'p="%p"; f="%f"; shopt -s nullglob; for d in ${PGDATA%%%%/}.wals.*; do test ! -f "$d/$f" && cp "$p" "$d/$f.tmp$$" && mv "$d/$f.tmp$$" "$d/$f"; done'
        if self.dbinstancebackupprocess.conf.values['archive_command'] != f"'{archive_command_string}'":
            raise Exception(f"Change archive_command to '{archive_command_string}'")
        if 'archive_timeout' not in self.dbinstancebackupprocess.conf.values or int(self.dbinstancebackupprocess.conf.values['archive_timeout']) > 600:
            raise Exception("Change archive_timeout to 600 or lower")

        # Create WAL directory
        waldir = self.dbinstancebackupprocess.pgdata.parent / f"{self.dbinstancebackupprocess.pgdata.name}.wals.{self.base_timestamp}"
        os.mkdir(waldir, mode=0o700)
        os.chown(waldir, self.dbinstancebackupprocess.service.user_uid, self.dbinstancebackupprocess.service.user_gid)
        self.walspool = PostgreSQLWalSpool(self.archive_cycle, waldir)

        self.pgdata = BackupDir(self.dbinstancebackupprocess.pgdata)

        self._logger.info("Starting backup.")

        try:
            backup_label = None
            with PostgreSQLBackupContext(self.dbinstancebackupprocess.get_superuser_connstring(), f"pgbak{self.base_timestamp}", uid=self.dbinstancebackupprocess.service.user_uid, gid=self.dbinstancebackupprocess.service.user_gid, groups=[self.dbinstancebackupprocess.service.user_gid]) as backup_context:
                backup_context.start()
                self.pgdata.snap()
                backup_label = backup_context.stop()

            # Store backup_label
            backup_label_path = self.pgdata.snap_path / "backup_label"
            with open(backup_label_path, "w") as backup_label_fd:
                # backup_label_fd.write(db_connection.backup_label)
                backup_label_fd.write(backup_label)
            os.chown(backup_label_path, self.dbinstancebackupprocess.service.user_uid, self.dbinstancebackupprocess.service.user_gid)

            # Backup snapshot
            self.archive_cycle.archive_base(self.pgdata.snap_path)

        finally:

            # Release snapshot
            self.pgdata.free()

        # Fail here is WAL files aren't coming in.
        self.walspool.wait_for_archive(timeout=60)

        self._logger.info("Creating backup archive.")

        # Archive wals
        self.do_backup()

        self._logger.info("Done.")

    def do_backup(self):
        self.walspool.do_spool()
        if self.archive_cycle.wal_heavy:
            self.is_expired = True

    def do_backup_and_end(self):
        self.is_expired = True
        # By this point a new base backup and new wal sequence should exist.
        self.walspool.do_backup_and_end()


## Borg utils

class BorgClient():
    def __init__(self, env):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.env = {}
        self.env.update(os.environ)
        self.env.update(env)
        self._logger.info("Connecting to borg repo...")
        check_output(["borg", "info"], stdin=DEVNULL, env=self.env)
        self.env_cmd = []
        if self.env:
            self.env_cmd = ["env"] + [f"{key}={value}" for (key,value) in self.env.items() if key not in os.environ or value != os.environ[key]]

    def archive(self, archive, cwd, *contents, exclude=[]):
        arch = BorgArchive(archive, self)
        start_time = datetime.datetime.now()
        self._logger.info(f"Creating borg archive: {archive}")
        check_call(["borg", "create"] + list(itertools.chain(*[("--exclude", e) for e in exclude])) + [f"::{archive}"] + [pathlib.Path(c).as_posix() for c in contents], cwd=None if cwd is None else pathlib.Path(cwd).as_posix(), stdin=DEVNULL, env=self.env)
        self._logger.info(f"Archive created.")
        end_time = datetime.datetime.now()
        arch.archive_elapsed_time = end_time - start_time
        arch.get_info()
        return arch

    def get_create_from_stdin_cmd(self, archive, stdin_name=None):
        return self.env_cmd + ["borg", "create", "--stdin-name", stdin_name, f"::{archive}", "-"]

    def extract(self, archive, cwd, *contents):
        self._logger.info(f"Extracting borg archive: {archive}")
        check_call(["borg", "extract", "--sparse", f"::{archive}"] + [pathlib.Path(c).as_posix() for c in contents], cwd=None if cwd is None else pathlib.Path(cwd).as_posix(), stdin=DEVNULL, env=self.env)
        self._logger.info(f"Archive extracted.")

    def get_stream_command(self, archive, *contents):
        return self.env_cmd + ["borg", "extract", "--stdout", f"::{archive}"] + [pathlib.Path(c).as_posix() for c in contents]

    def get_archive_info(self, name):
        outs = check_output(["borg", "info", "--json", f"::{name}"], stdin=DEVNULL, env=self.env)
        return json.loads(outs)['archives'][0]

    def delete(self, name):
        self._logger.info(f"Deleting borg archive: {name}")
        check_call(["borg", "delete", f"::{name}"], stdin=DEVNULL, env=self.env)

    def rename(self, oldname, newname):
        self._logger.info(f"Renaming borg archive: {oldname} {newname}")
        check_call(["borg", "rename", f"::{oldname}", newname], stdin=DEVNULL, env=self.env)

    def archives(self, prefix=None):
        outs = check_output(["borg", "list", "--json"] + ([] if prefix is None else ["--prefix", prefix]), stdin=DEVNULL, universal_newlines=True, env=self.env)
        archive_data = json.loads(outs)
        return {BorgArchive(arch['name'], self) for arch in archive_data['archives']}

    def expire_prefix(self, prefix):
        check_call(["borg", "prune", "--keep-within", "1d", "--keep-hourly", "24", "--keep-daily", "7", "--keep-weekly", "52", "--keep-monthly", "12", "--keep-yearly", "30", "--prefix", prefix, f"::"], stdin=DEVNULL, universal_newlines=True, env=self.env)

class BorgArchive():
    def __init__(self, name, borg):
        self.name = name
        self.borg = borg

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def get_info(self):
        data = self.borg.get_archive_info(self.name)
        self.size = data['stats']['original_size']
        self.compressed_size = data['stats']['compressed_size']

    def extract(self, cwd=None):
        self.borg.extract(self.name, cwd=cwd)

    def get_stream_command(self, *content):
        return self.borg.get_stream_command(self.name, *content)

    def delete(self):
        self.borg.delete(self.name)
        self.name = None
        self.borg = None


## Filesystem utils

class BackupDir():
    def __init__(self, d):
        self._logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.path = d
        self._snap = None

    def snap(self):
        if self._snap:
            return
        self._logger.info("Creating snapshot...")
        self._snap = CowSnap(self.path)
        self._logger.info("Done.")

    @property
    def snap_path(self):
        return None if self._snap is None else self._snap.snap_path

    def free(self):
        if self._snap:
            self._logger.info("Deleting snapshot.")
            self._snap.free()
            self._snap = None
        self._logger.info("Done.")

class CowSnap():
    def __init__(self, path):
        self.path = path
        td = TemporaryDirectory(dir = self.path.parent, prefix = f".snap.")
        d = pathlib.Path(td.name) / self.path.name
        try:
            # Future: Python 3.8+ has copytree() which can do CoW, but we need to support 3.6.
            check_call(["cp", "-a", "--reflink=always", "--no-dereference", self.path.as_posix(), d.as_posix()], stdin=DEVNULL)
            self.td = td
            self.snap_path = d
            td = None
        finally:
            if td is not None:
                td.cleanup()

    def free(self):
        self.td.cleanup()


## systemd interface

class SystemServices():
    def __init__(self):
        units = check_output(["systemctl", "list-units", "-t", "service", "--full", "--all", "--plain", "--no-legend"], stdin=DEVNULL, universal_newlines=True, timeout=60)
        self.unit_names = set([unit.split(" ")[0] for unit in units.split("\n")])
        self.postgresql_services = [Service(name) for name in self.unit_names if re.match(r"^postgresql(|-\d+)(|@\w+)\.service$", name)]

    def get_backup_postgresql_services(self):
        return [service for service in self.postgresql_services if service.is_enabled() or service.is_active()]

    def get_restore_postgresql_services(self, instance=None, pgversion=None):
        return [service for service in self.postgresql_services if (service.instance_or_default == instance or instance == None) and (service.pgversion == pgversion or pgversion == None)]

class Service():
    def __init__(self, name):
        self.name = name
        cat = check_output(["systemctl", "cat", self.name], stdin=DEVNULL, universal_newlines=True, timeout=10)
        section = None
        env = {}
        self.user = None
        self.group = None
        for line in cat.split("\n"):
            l = line.strip()
            if l.startswith("["):
                section = l
            if section == "[Service]":
                if l.startswith("Environment="):
                    (x,var,val) = l.split("=",2)
                    env[var.strip()] = val.strip()
                if l.startswith("User="):
                    (x,val) = l.split("=",1)
                    self.user = val.strip()
        self.environment = env
        if self.user is not None:
            self.user_uid = getpwnam(self.user).pw_uid
            self.user_gid = getpwnam(self.user).pw_gid

    @property
    def pgversion(self):
        m = re.match(r"^postgresql(|-\d+)(|@\w+)\.service$", self.name)
        if not m:
            raise Exception(f"Service name mismatch: {self.name}")
        if m.group(1) == "":
            return "default"
        return m.group(1).split("-")[1]

    @property
    def instance(self):
        if "@" in self.name:
            return self.name.split("@")[1].split(".")[0]
        return None

    @property
    def instance_or_default(self):
        i = self.instance
        if i is None:
            return "default"
        return i

    def is_enabled(self):
        try:
            check_output(["systemctl", "is-enabled", self.name], stdin=DEVNULL, timeout=10)
            return True
        except CalledProcessError as e:
            pass
        return False

    def is_active(self):
        try:
            check_output(["systemctl", "is-active", self.name], stdin=DEVNULL, timeout=10)
            return True
        except CalledProcessError as e:
            pass
        return False

class NotifyStatus():
    def __init__(self):
        self.last_status_text = None
        self.last_status_time = None
        self.sent_ready = False
        self.watchdog_usec = None
        self.sleep_time = 60
        if 'WATCHDOG_USEC' in os.environ:
            self.watchdog_usec = int(os.environ['WATCHDOG_USEC'])
            self.sleep_time = min(self.watchdog_usec / 3000000, self.sleep_time)

    def set_ready(self):
        if not self.sent_ready:
            notify("READY=1")
            self.sent_ready = True

    def set_status(self, s):
        def monotonic_clock():
            return clock_gettime(CLOCK_MONOTONIC)

        if self.last_status_text is None or s != self.last_status_text or monotonic_clock() - self.last_status_time >= self.sleep_time:
            notify(f"STATUS={s}")
            self.last_status_text = s
            self.last_status_time = monotonic_clock()
            if self.watchdog_usec:
                notify("WATCHDOG=1")


## FIXME replace with threading module

class StopHandler():
    def __init__(self, callback=None):
        self.stop = False
        self._callback = callback

        def signalled_stop(*args, **kwargs):
            self.stop = True
            if self._callback is not None:
                self._callback(*args, **kwargs)

        signal.signal(signal.SIGINT, signalled_stop)
        signal.signal(signal.SIGTERM, signalled_stop)

    # TODO there is probably a better way
    def sleep(self, s):
        while s > 1:
            if self.stop:
                break
            sleep(5)
            s -= 5
        sleep(s)

class CountDown():
    def __init__(self, t):
        self.reset(t=t)

    def reset(self, t=None):
        if t is not None:
            self._t = t
        self._liftoff_time = clock_gettime(CLOCK_MONOTONIC) + self._t

    @property
    def reached_liftoff(self):
        return self._liftoff_time <= clock_gettime(CLOCK_MONOTONIC)

class StopWatch():
    def __init__(self):
        self._start = clock_gettime(CLOCK_MONOTONIC)

    @property
    def total(self):
        return clock_gettime(CLOCK_MONOTONIC) - self._start

