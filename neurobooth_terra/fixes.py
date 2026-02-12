import socket

from sshtunnel import SSHTunnelForwarder


class OptionalSSHTunnelForwarder(SSHTunnelForwarder):
    """SSH tunneling, skipped if already on neurodoor."""

    def __enter__(self):
        if (socket.gethostname() == 'neurodoor.nmr.mgh.harvard.edu') or (socket.gethostname() == 'neurodoor2.nmr.mgh.harvard.edu'):
            return self
        return SSHTunnelForwarder.__enter__(self)

    @property
    def local_bind_port(self):
        if socket.gethostname() == 'neurodoor.nmr.mgh.harvard.edu' or (socket.gethostname() == 'neurodoor2.nmr.mgh.harvard.edu'):
            return '5432'
        return super(OptionalSSHTunnelForwarder, self).local_bind_port

    @property
    def local_bind_host(self):
        if socket.gethostname() == 'neurodoor.nmr.mgh.harvard.edu' or (socket.gethostname() == 'neurodoor2.nmr.mgh.harvard.edu'):
            return 'localhost'
        return super(OptionalSSHTunnelForwarder, self).local_bind_host

    def __exit__(self, exc_type, exc_value, traceback):
        if socket.gethostname() == 'neurodoor.nmr.mgh.harvard.edu' or (socket.gethostname() == 'neurodoor2.nmr.mgh.harvard.edu'):
            return False
        return SSHTunnelForwarder.__exit__(self, exc_type, exc_value,
                                           traceback)
