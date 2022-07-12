import time
import json
import copy



def execute_function(grid_data,id):
    import numpy as np
    import os
    import psutil
    import time
    from pynq import Overlay, allocate, Device, lib

    from FWIDriver import FWI
    
    start_time = time.time()
    
    # Set up the configuration
    
    config = single_cu_config
    path = XCLBIN_PATH_5000
    
    
    gridsize = 100
    resolution = config["Freq"]["nTotal"] * config["nSources"] * config["nReceivers"]
    config["tolerance"] = 9.99*10**-7
    config["max"] = 1000
    cu=1
    acceleration = True

    if acceleration:
        
        # Load the overlay
        devices = Device.devices

        # Get the Overlay
        #TODO: add device name to config file
        ol = Overlay(path, download=True, device=devices[0])

        # Allocate the buffers
        A = allocate(shape=(resolution,gridsize), dtype=np.complex64, target=ol.DDR0)
        B = allocate(shape=(gridsize,), dtype=np.float32, target=ol.DDR0)
        C = allocate(shape=(resolution,), dtype=np.complex64, target=ol.DDR0)

        D = allocate(shape=(resolution,gridsize), dtype=np.complex64,  target=ol.DDR1)
        E = allocate(shape=(resolution,),dtype=np.complex64,  target=ol.DDR1)
        F = allocate(shape=(gridsize), dtype=np.complex64, target=ol.DDR1)

        # set up the kernel IP's
        dotprod = getattr(ol,"dotprod_"+str(1))
        update = getattr(ol,"update_"+str(1))

        print(f"allocation took: {time.time()-start_time}")
        # Execute the Full Waveform Inversion algorithm
        fwi = FWI(A,B,C,D,E,F,dotprod,update,config,resolution,gridsize,acceleration)
    else:
        fwi = FWI(config=config,resolution=resolution,gridsize=gridsize,acceleration=acceleration)

    s = 0
    e = 100
    
    for i in range(int(100/gridsize)): 
        fwi.pre_process(grid_data[s:e])
        
        # reconstruct the grid by performing Full Wavefrom Inversion
        chi = fwi.reconstruct()
        s = e
        e += 100

    if acceleration:
        # free all the buffers
        A.freebuffer()
        B.freebuffer()
        C.freebuffer()
        D.freebuffer()
        E.freebuffer()
        F.freebuffer()
        
    # Return statistics and results from FWI
    
    total_time = time.time() - start_time
    
    dict_t = {
    "id": id,
    "cu": cu,
    "dot": fwi.model.dot_time,
    "upd": fwi.inverse.updtime,
    "time": total_time,
#     "memory": psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)

    }
    return dict_t
    
# Load in data and config settings
original_data = []
with open("default/input/"+"10x10_100"+".txt") as f:
    for l in f:
        original_data.append(float(l))
        

with open("default/input/GenericInput.json") as f:
    fwi_config = json.load(f)

#set specific configurations for different types of kernels
single_cu_config = fwi_config
double_cu_config = copy.deepcopy(fwi_config)
single_cu_config["ngrid"]["x"]=10
single_cu_config["ngrid"]["z"]=10
single_cu_config["Freq"]["nTotal"]=12
single_cu_config["nSources"]=20
single_cu_config["nReceivers"]=20
XCLBIN_PATH_5000 = "bitstreams/5000_100_1CU/5000_100_1cu.xclbin"


t = time.time()
r = execute_function(original_data,1)

print(f"total: {time.time()-t}")

print(r)
