import paramiko
import select
import socket
import SocketServer
import logging

def make_forward_handler(remote_address_, transport_, log_):
    '''
    Wrapper to assign vars
    '''
    class Handler(ForwardHandler):
        remote_address = remote_address_
        ssh_transport = transport_
        log = log_
    return Handler        

class ForwardHandler(SocketServer.BaseRequestHandler):
    '''
    Base handler for tunnel connections
    '''

    def _redirect(self, chan):
        while True:
            rqst, __, __ = select.select([self.request, chan], [], [], 5)
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
            self.log.debug("Opening channel from {0} to {1}".format(src_address, self.remote_address))
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
    timeout = 5

    def __init__(self, server_address, RequestHandlerClass, log):
        self.log = log
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)

    def handle_timeout(self):
        self.log("Timeout reached, closing forward tunnel server")
        self.server_close()
