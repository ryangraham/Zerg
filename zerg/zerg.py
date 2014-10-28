#!/usr/bin/python
import gevent
from gevent.pool import Pool
import argparse
import paramiko
import socket


class Zerg:

    def __init__(self, hosts, username, key, max_threads=2):
        self.hosts = hosts
        self.username = username
        self.key = paramiko.RSAKey.from_private_key_file(key)
        self.pool = Pool(max_threads)
        self.connections = []

    def _connect(self, host):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect(host, username=self.username, pkey=self.key)
        self.connections.append(client)

    def connect(self):
        self.pool.map(self._connect, self.hosts)

    def _command(self, cmd, conn):
        stdin, stdout, stderr = conn.exec_command(cmd)
        rc = stdout.channel.recv_exit_status()
        lines = stdout.read().splitlines()
        return rc, lines

    def command(self, cmd):
        out = self.pool.map(lambda c: self._command(cmd, c), self.connections)
        return out


def cmdline_helper():

    def validate_hosts(h):
        """
        Validate comma delimited IP addresses
        """
        addresses = h.split(',')
        for addr in addresses:
            try:
                socket.inet_aton(addr)
            except socket.error:
                raise argparse.ArgumentTypeError("Bad address: %s" % addr)
        return addresses

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', type=str, required=True,
                        help="Username for remote host")
    parser.add_argument('-i', '--pkey', type=str, required=True,
                        help="Path to private key (ie ~/.ssh/id_rsa)")
    parser.add_argument('--hosts', type=validate_hosts, required=True,
                        help="Target IP addresses (comma delimited)")
    parser.add_argument('-c', '--command', type=str, required=True,
                        help="Batch commands to run against host")
    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = cmdline_helper()
    hosts = args.hosts
    username = args.username
    key = args.pkey
    command = args.command

    lings = Zerg(hosts, username, key)
    lings.connect()
    results = lings.command(command)
    for r in results:
        print "--"
        print "Return code: %d" % r[0]
        print "Result: %s" % r[1]
        print "--\n"
