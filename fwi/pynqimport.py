from pynq import Device, Overlay
import json

dev = Device.devices
with open("cluster_config.json") as f:
	config = json.load(f)

ol = Overlay(config["overlay"],download=True,device=dev[0])
