from pynq import Device, Overlay
import json
import socket
  

dev = Device.devices
with open("cluster_config.json") as f:
	config = json.load(f)

overlay = config["overlay"]
if isinstance(overlay,list):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.254.254.254', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    overlay = overlay[config["hosts"].index(IP)]
                      
        
ol = Overlay(overlay,download=True,device=dev[0])
