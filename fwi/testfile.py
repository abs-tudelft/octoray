from pynq import Device, Overlay, allocate


resolution = 500
gridsize = 250

A = allocate(shape=(resolution,gridsize), dtype=np.complex64, target=getattr(ol,kernel["functions"][0]["dotprod_"+str(cu)][0]))
B = allocate(shape=(gridsize,), dtype=np.float32, target=getattr(ol,kernel["functions"][0]["dotprod_"+str(cu)][1]))
C = allocate(shape=(resolution,), dtype=np.complex64, target=getattr(ol,kernel["functions"][0]["dotprod_"+str(cu)][2]))

D = allocate(shape=(resolution,gridsize), dtype=np.complex64,  target=getattr(ol,kernel["functions"][1]["update_"+str(cu)][0]))
E = allocate(shape=(resolution,),dtype=np.complex64,  target=getattr(ol,kernel["functions"][1]["update_"+str(cu)][1]))
F = allocate(shape=(gridsize), dtype=np.complex64, target=getattr(ol,kernel["functions"][1]["update_"+str(cu)][2]))

