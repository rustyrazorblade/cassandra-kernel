import re
from ipykernel.kernelbase import Kernel
import sys
from cassandra.query import dict_factory

import matplotlib
# matplotlib.style.use('ggplot')
from base64 import b64encode

from StringIO import StringIO

print sys.executable
print sys.argv

from logging import warn

from cassandra.cluster import Cluster
from pandas import DataFrame

connect = re.compile("%connect (.*)")
line_graph = re.compile("%line (.*)")

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
        connect_str = connect.search(code)

        if connect_str:
            address = connect_str.groups()[0]
            self.cluster = Cluster([address])
            self.cassandra_session = self.cluster.connect("test")
            self.cassandra_session.row_factory = dict_factory

            self.send_response(self.iopub_socket, 'stream', {"name":"stdout", "text":"connected"})
            return {"status": "ok",
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {} }

        if not self.cluster:
            warn("No connection")
            return {"status": "ok",
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}  }
        # if we don't have a connection, complain
        # if it's a query, execute
        # it could be a histogram, or line graph


        lg = line_graph.search(code)

        if lg:
            result = self.cassandra_session.execute(lg.groups()[0])
            df = DataFrame(result)
            p = df.plot(kind='line')
            fig = p.get_figure()
            s = StringIO()
            fig.savefig(s, format="png")
            s.seek(0)

            # do line graph stuff
        
            stream_content = {"name":"stdout",
                              "data": {
                                "image/png": b64encode(s.buf)
                              },
                              "metadata": {}
                              }
            self.send_response(self.iopub_socket, 'display_data', stream_content)
            return {'status': 'ok',
                    # The base class increments the execution count
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {},
                   }

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
