import json
import numpy as np
import asyncssh
import dask
from dask.distributed import Client, progress, get_worker
import os
import time
from multiprocessing import Process
from octoray.MySSHCluster import MySSHCluster

    
class Octoray():
    def __init__(self, ssh_cluster=False, scheduler_ip="10.1.212.126",scheduler_port="8786",hosts=["10.1.212.129"]):
        #TODO: add list of ip address for workers so we can automatically spawn workers.
        print(f"Initializing OctoRay with client ip: {scheduler_ip}")  
        self.cluster = f"tcp://{scheduler_ip}:{scheduler_port}"
        self.ssh_cluster = ssh_cluster
        self.hosts = hosts
        
        if self.cluster:
#             dask.config.set({"distributed.worker.daemon":False})
            self.cluster = self.start_cluster(hosts,scheduler_ip)
            
        self.scheduler_ip = scheduler_ip
        self.client = Client(self.cluster)
        
        print("Waiting until workers are set up on remote machines...")
        while len(self.client.scheduler_info()["workers"]) == 0:
            time.sleep(0.1)
        
        self.num_of_workers = len(self.client.scheduler_info()["workers"])
        print(f"Current amount of workers: {self.num_of_workers}")
        
        self.kernels = []
            

    def close_cluster(self):
#         self.p.terminate()
#         if self.ssh_cluster:
#             self.cluster.shutdown()
#             self.close_workers()
        self.client.close()
        print("killed cluster and client")
        
    def start_cluster(self,hosts,scheduler):
        self.cluster = MySSHCluster(
            scheduler,
            8786,
            hosts,
            1,
            1,
            None,
            22,
            None,
            None,
            None,
            None,
            0,
            0,
            0,
            "distributed.cli.dask_worker"
        )
        time.sleep(0.5)
        return self.cluster
    
    def execute_cluster(self,data_split,kernels,func):
        d_data = self.client.scatter(data_split)
        d_kernels = self.client.scatter(kernels)
        futures = self.client.map(func,d_data,d_kernels,range(len(self.client.scheduler_info()["workers"])))
        res = self.client.gather(futures)
        return res
            
            
    async def close_workers(self):
        #TODO: EXPERIMENTAL FUNCTION, NOT TESTED
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

    def add_kernels(self,*args):
        """Kernels that are added will be executed on available workers."""
        for i in args:
            self.kernels.append(i)
        return args

    def print_kernels(self):
        for i in self.kernels:
            print(i)
            
    def split_dataset(self, dataset:list):
        #check that we only process the amount of kernels as workers that we have.
        self.num_of_workers = len(self.client.scheduler_info()["workers"])
        print(self.num_of_workers)
        if self.num_of_workers == 0:
            print("There are no workers available, exiting...")
            sys.exit(1)
        elif len(self.kernels)>self.num_of_workers:
            print("There are more kernels added to Octoray than workers available, removing the excesive kernels...")

            self.kernels = self.kernels[:self.num_of_workers]            
        self.data_split = []
        start = 0
        for krnl in self.kernels:
            krnl_set = []
            for i in range(krnl["no_instances"]):
                krnl_set.append(dataset[start:start+krnl["batch_size"]])
                start += krnl["batch_size"]
            self.data_split.append(krnl_set)
        return self.data_split               
    
if __name__ == "__main__":
    octo = Octoray()
    
