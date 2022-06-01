from tcpclient import TCPClient

tcp_client = TCPClient("0.0.0.0", 5555)
tcp_client.connect()
while True:
    if tcp_client.socket is not None:
        data = tcp_client.socket.recv(1024)
        print(data)
    else:
        break
        # Create Spark Stream Processor, buffer 20
