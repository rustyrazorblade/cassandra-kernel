import re
from ipykernel.kernelbase import Kernel
import sys
from cassandra.query import dict_factory

print sys.executable
print sys.argv

from cassandra.cluster import Cluster
from pandas import DataFrame

connect = re.compile("%connect (.*)")

class CassandraKernel(Kernel):
    implementation = 'Cassandra'
    implementation_version = '1.0'
    language = 'cql'
    language_version = '0.1'
    language_info = {'mimetype': 'text/plain', "name":"cql"}
    banner = "Cassandra CQL 3 Kernel"

    cluster = None
    cassandra_session = None

    def do_execute(self, code, silent,
                   store_history=True, user_expressions=None,
                   allow_stdin=False):

        # check to see if it's a %connect
        if not self.cluster:
            self.cluster = Cluster()
            self.cassandra_session = self.cluster.connect("test")
            self.cassandra_session.row_factory = dict_factory

        # if we don't have a connection, complain
        # if it's a query, execute
        # it could be a histogram, or line graph

        result = self.cassandra_session.execute(code)
        df = DataFrame(result)

        if not silent:
            stream_content = {'name': 'stdout', 'text': str(df)}
            self.send_response(self.iopub_socket, 'stream', stream_content)

        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
               }


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=CassandraKernel)
