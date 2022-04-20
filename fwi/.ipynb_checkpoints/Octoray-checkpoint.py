import json
import numpy as np
import asyncssh
import dask
from dask.distributed import Client, progress, get_worker
import os
import time
import copy

from multiprocessing import Process
from MySSHCluster import MySSHCluster

    
class Octoray():
    def __init__(self, ssh_cluster=False, scheduler="10.1.212.126",scheduler_port="8786",hosts=[]):
        #TODO: add list of ip address for workers so we can automatically spawn workers.
        #TODO: add config file for ssh configurations
        self.kernels = []
        self.ssh_cluster = ssh_cluster
        self.scheduler = scheduler
        self.scheduler_port = scheduler_port
        self.hosts = hosts

    def create_cluster(self):
        print(f"Initializing OctoRay with client ip: {self.scheduler}")  
        self.cluster = f"tcp://{self.scheduler}:{self.scheduler_port}"
        if self.ssh_cluster:
            dask.config.set({"distributed.worker.daemon":False})
            self.cluster = self.cluster = MySSHCluster(scheduler_addr=self.scheduler,scheduler_port=self.scheduler_port,workers=self.workers,
                                                        nthreads=1,
                                                        ssh_username=None,
                                                        ssh_port=22,
                                                        ssh_private_key=None,
                                                        nohost=False, 
                                                        logdir=None,
                                                        remote_python=None,
                                                        memory_limit=None,
                                                        worker_port=0,
                                                        nanny_port=0,
                                                        remote_dask_worker="distributed.cli.dask_worker",
                                                        local_directory=None
                                                    )
        
        self.client = Client(self.cluster)
        print("Waiting until workers are set up on remote machines...")
      
        timeout = time.time() + 15
        while len(self.client.scheduler_info()["workers"]) != len(self.kernels):
            time.sleep(0.1)
            if time.time() > timeout:
                raise TimeoutError("Timed out after 15 seconds... exiting")
        
        self.num_of_workers = len(self.client.scheduler_info()["workers"])
        print(f"Current amount of workers: {self.num_of_workers}")
        
            
    def close_cluster(self):
#         self.p.terminate()
#         if self.ssh_cluster:
#             self.cluster.shutdown()
#             self.close_workers()
        self.client.close()
        print("killed cluster and client")

    def setup_cluster(self,data, *kernels):
        """Kernels that are added will be executed on available workers."""
        for i in kernels:
            self.kernels.append(i)    
        #check that we only process the amount of kernels as workers that we have.  
        if len(self.hosts) == 0:
            raise ValueError("There are no hosts available, please add at least one host.")
        elif len(kernels)>len(self.hosts):
            raise ValueError(f"There are more kernels ({len(kernels)}) added to Octoray than hosts ({len(self.hosts)}) available, Add more hosts or remove the excessive kernels...")
        
        #create worker objects
        self.workers = []
        for i, krnl in enumerate(self.kernels):
            worker = {"addr": self.hosts[i],
                      "n_workers":1, #krnl["no_instances"]
                     }
            self.workers.append(worker)
            
        data_split, kernels_split = self.split_data_and_kernels(data,self.kernels)
        
        #create the cluster
        self.create_cluster()
        
        return data_split, kernels_split
    
    def execute_function(self,func,data_split,kernels_split):
        d_data = self.client.scatter(data_split)
        d_kernels = self.client.scatter(kernels_split)
        futures = self.client.map(func,d_data,d_kernels,range(len(self.client.scheduler_info()["workers"])))
        res = self.client.gather(futures)
        return res
            
            
    async def close_workers(self):
        #TODO: EXPERIMENTAL FUNCTION shuts down kernel as well
        for h in self.hosts:
            async with asyncssh.connect(h,22) as conn:
                res = await conn.run("lsof -n -i | grep 8786 | awk '{system(\"kill \" $2)}'",check=True)
                print(res.stdout,end='')

    def create_kernel(self, path:str, no_instances:int=1, batch_size:int=0, func_specs:list=[],config=None):
        """Creates a dictionary that represents a kernel.
        @param path: The path to the bitsream
        @param no_instances: If there are copied instances (default = 1)
        @param batch_size: The amount of data each compute unit should process.
        @param func_specs: The functions inside the kernel with their memory specifications
            A functions square_numbers(double a, double b) where a is mapped to HBM0 and b to HBM1
            is represented as: [{"square_numbers":[HBM0,HBM1]}]
        @param config: If necessary a configuration file or variable can be added.
        """ 
        kernel = {
            "path_to_kernel":path,
            "no_instances":no_instances,
            "batch_size":batch_size,
            "functions":func_specs
            }    
        if config:
            kernel["config"]=config
            
        return kernel

    def print_kernels(self,kernels):
        for i in kernels:
            print(i)
            print("\n")
            
    def split_data_and_kernels(self, dataset:list,kernels):
        
        self.data_split = []
        start = 0
        for krnl in kernels:
            krnl_set = []
            for i in range(krnl["no_instances"]):
                krnl_set.append(dataset[start:start+krnl["batch_size"]])
                start += krnl["batch_size"]
            self.data_split.append(krnl_set)
        return self.data_split, kernels            
    
    
    def split_kernels(self,kernels):
        #TODO: separate kernels can't program device, maybe use this code for multidevice on 1 host.
        #create a separate kernel for each instance
        for i, krnl in enumerate(kernels):
            if krnl["no_instances"]>1:
                for t in range(krnl["no_instances"]-1):
                    new_kernel = copy.deepcopy(krnl) #need to deepcopy so we don't overwrite functions
                    new_kernel["no_instances"]=1
                    new_kernel["functions"]= krnl["functions"][t+1]
                    kernels.insert(i+1,new_kernel)
                krnl["functions"] = krnl["functions"][0]
                krnl["no_instances"] = 1
if __name__ == "__main__":
    octo = Octoray()