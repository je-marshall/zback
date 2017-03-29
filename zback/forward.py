import paramiko
import select
import socket
import SocketServer
import logging

class _ForwardHandler(SocketServer.BaseRequestHandler):
    '''
    Base handler for tunnel connections
    '''

    remote_address = None
    ssh_transport = None
    log = None

    def _redirect(self, chan):
        while True:
            rqst, __, __ = select([self.request, chan], [], [], 5)
            if self.request in rqst:
                data = self.request.recv(1024)
                chan.send(data)
                if len(data) == 0:
                        break
            if chan in rqst:
                data = chan.recv(1024)
                self.request.send(data)
                if len(data) == 0:
                        break
    
    def handle(self):
        src_address = self.request.getpeername()

        try:
            chan = self.ssh_transport.open_channel(
                kind='direct-tcpip',
                dest_addr=self.remote_address,
                src_addr=src_address,
                timeout=10
            )
        except paramiko.SSHException:
            raise

        try:
            self._redirect(chan)
        except socket.error:
            pass
        except Exception as e:
            self.log.error(e)

        finally:
            chan.close()
            self.request.close()

class _UnixStreamForwardServer(SocketServer.UnixStreamServer):
    '''
    Serve over UNIX domain sockets, avoiding tunneling TCP over TCP
    '''

    def __init__(self, *args, **kwargs):
        self.log = logging.getLogger('zback.forward')
        SocketServer.UnixStreamServer.__init__(self, *args, **kwargs)

    @property
    def local_address(self):
        return self.server_address

    @property
    def local_host(self):
        return None
    
    @property
    def local_port(self):
        return None

    @property
    def remote_address(self):
        return self.RequestHandlerClass.remote_address

    @property
    def remote_host(self):
        return self.RequestHandlerClass.remote_address[0]

    @property
    def remote_port(self):
        return self.RequestHandlerClass.remote_address[1]

def _make_forward_handler(remote_address_, transport_):
    '''
    Wrapper to assign vars
    '''
    class Handler(_ForwardHandler):
        remote_address = remote_address_
        transport = transport_
    return Handler        

def _make_forward_server(remote_address, local_address, transport):
    '''
    SSH forward proxy class
    '''

    _Server = _UnixStreamForwardServer(remote_address)
    _Handler = _make_forward_handler(remote_address, transport)
    ssh_forward_server = _Server(local_address, _Handler)

    if ssh_forward_server:
        return ssh_forward_server
    else:
        raise RuntimeError("Could not establish server")


## So this is how I think it should work

class ForwardHandler(SocketServer.BaseRequestHandler):
    '''
    Base handler for tunnel connections
    '''

    remote_address = None
    ssh_transport = None
    log = None

    def _redirect(self, chan):
        while True:
            rqst, __, __ = select([self.request, chan], [], [], 5)
            if self.request in rqst:
                data = self.request.recv(1024)
                chan.send(data)
                if len(data) == 0:
                        break
            if chan in rqst:
                data = chan.recv(1024)
                self.request.send(data)
                if len(data) == 0:
                        break

    def handle(self):
        src_address = self.request.getpeername()

        try:
            chan = self.ssh_transport.open_channel(
                kind='direct-tcpip',
                dest_addr=self.remote_address,
                src_addr=src_address,
                timeout=10
            )
        except paramiko.SSHException:
            raise

        try:
            self._redirect(chan)
        except socket.error:
            pass
        except Exception as e:
            self.log.error(e)

        finally:
            chan.close()
            self.request.close()

class TCPServer(SocketServer.TCPServer):
    pass