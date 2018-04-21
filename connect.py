# -*- coding: utf-8 -*-

import contextlib
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import THBaseService
from hbase.ttypes import TGet


@contextlib.contextmanager
def connect():
    transport = TTransport.TBufferedTransport(TSocket.TSocket('127.0.0.1', 9090))
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = THBaseService.Client(protocol)
    transport.open()
    try:
        yield client
    finally:
        transport.close()


if __name__ == '__main__':
    with connect() as client:
        tget = TGet(row='sys.cpu.user:20180421:192.168.1.1')
        tresult = client.get("tsdata", tget)
        print(tresult)
