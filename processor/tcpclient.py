import socket
import time

from logger import logger


class TCPClient:
    """
    Initialize a TCP client socket with given host and port
    ...

    Attributes
    ----------
    host : str
        Server's host name or IP
    port : str
        Server's port
    retry_attempts : int
        Max attempt count to connect with server, a default value but can also be fed in
    socket: socket
        A socket object using socket.socket()

    Methods
    -------
    connect(attempts=1)
        Initialize a socket connection
    """

    def __init__(self, host, port, retry_attempts=5):
        self.host = host
        self.port = port
        self.retry_attempts = retry_attempts
        self.socket = None

    def connect(self, attempts=1):
        """
        Initialize a socket connection, retries if can't find server.
        Sends hi message to server after connecting
        :param attempts: increases for each unsuccessful connection attempt until it reaches retry_attempts value.
        """
        if attempts <= self.retry_attempts:
            try:
                self.socket = socket.socket()
                self.socket.connect((self.host, self.port))
                self.socket.sendall(b"Hi, here to listen")
                logger.info(f"""Successfully connected to {self.host}, on port {self.port}""")
            except socket.error as e:
                logger.error(f"""Cannot connect to socket, caught exception socket.error : {e}""")
                logger.error(f"""Retrying attempt: {attempts} """)
                time.sleep(1)
                self.connect(attempts + 1)

        else:
            logger.error("Terminating connection attempts, all attempts failed!")
            self.socket = None
