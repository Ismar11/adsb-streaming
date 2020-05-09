import socket
import json
from opensky_api import OpenSkyApi
import time

host = 'localhost'
port = 5555
api = OpenSkyApi()

class Server(object):
    
    def __init__(self,host,port):
        self._host = host
        self._port = port
        
    def __enter__(self):
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        sock.bind((self._host,self._port))
        sock.listen(10)
        self._sock = sock
        return self._sock
    
    def __exit__(self,*exc_info):
        
        if exc_info[0]:
            
            import traceback
            traceback.print_exception(*exc_info)
            
        self._sock.close()

with Server(host,port) as s:
    
    while True:
        
        conn, addr = s.accept()
        time.sleep(10)
        states = api.get_states(bbox=(45.8389, 47.8229, 5.9962, 10.5226))
        try:
            
            data = json.dumps([s.__dict__ for s in states.states]).encode('utf-8')
            #print("Sent :" + str(len(data)) + " bytes")
            conn.send(data)
            
        except AttributeError:
                
                print("Sent :" + "0 bytes")
        
        conn.close()