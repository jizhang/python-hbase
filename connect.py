# -*- coding: utf-8 -*-

import contextlib
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase


@contextlib.contextmanager
def connect():
    transport = TTransport.TBufferedTransport(TSocket.TSocket('127.0.0.1', 9090))
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Hbase.Client(protocol)
    transport.open()
    try:
        yield client
    finally:
        transport.close()


if __name__ == '__main__':
    with connect() as client:
        table_names = client.getTableNames()
        print(table_names)
