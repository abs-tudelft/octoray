import json
import numpy as np
import asyncssh
import dask
from dask.distributed import Client, progress
from SSHCluster import OctoSSHCluster
import os
import time
import copy

import subprocess

    
class Octoray():
    def __init__(self, ssh_cluster=False, config_file=None):
        self.kernels = []        
        if config_file:
            if isinstance(config_file,str):
                with open(config_file) as f:
                    self.config = json.load(f)
            if isinstance(config_file,dict):
                self.config = config_file
            self.scheduler = self.config["scheduler"]
            self.hosts = self.config["hosts"]    
            self.scheduler_port = self.config["scheduler_options"]["port"]
        else:
            raise ValueError("Configuration file or dict missing...")
            
        self.ssh_cluster = ssh_cluster
        self.setup_hosts = []
        self.worker_options = []
    
    def create_cluster(self):
        """Create the SSH cluster with a Scheduler and Worker(s). """
        
        self.check_hosts()
        self.check_kernels()
        
        self.num_of_workers = 0
        if isinstance(self.config["connect_options"],dict):
            self.num_of_workers = len(self.hosts)
        elif isinstance(self.config["connect_options"],list):
            for host in self.config["connect_options"]:
                self.num_of_workers += host["n_workers"]
            
        print(f"Initializing OctoRay with client ip: {self.scheduler}")
        
        self.cluster = f"tcp://{self.scheduler}:{self.scheduler_port}"
        if self.ssh_cluster:
            #Dask takes the first member in the hosts list as the scheduler so we add it here.            
            self.cluster = OctoSSHCluster(hosts=[self.scheduler,*self.hosts],
                                      connect_options=self.config["connect_options"],
                                      worker_options=self.worker_options,
                                      worker_class=self.config["worker_class"],
                                      scheduler_options=self.config["scheduler_options"]
                                     )

        self.client = Client(self.cluster)
        print("Waiting until workers are set up on remote machines...")
        
        timeout = time.time() + 15        
        while len(self.client.scheduler_info()["workers"]) < self.num_of_workers:
            time.sleep(0.1)
            if time.time() > timeout:
                raise TimeoutError("Timed out after 15 seconds... exiting")
        
        self.num_of_workers = len(self.client.scheduler_info()["workers"])
        
        print(f"Current amount of workers: {self.num_of_workers}")
        
    def setup_worker_options(self):
        if isinstance(self.config["worker_options"],dict):
            self.worker_options = [self.config["worker_options"]] * len(self.hosts)
        
    def shutdown(self):
        try:
            if self.ssh_cluster:
                self.cluster.close()
            self.client.close()
        except Exception as e:
            raise e

    def setup_cluster(self,data, *kernels):
        """Kernels that are added will be executed on available workers."""
        
        self.check_hosts()
        
        #Assign a host to the kernels
        for i, h in enumerate(self.hosts):
            kernels[i]["host"] = h
            self.kernels.append(kernels[i])

        self.check_kernels()
        self.setup_worker_options()
        
        kernels_split = self.split_kernels(self.kernels)
        data_split = self.split_data(data,kernels_split) 
        
        #create the cluster
        self.create_cluster()
        
        return data_split, kernels_split
    
    def execute(self,func,*args):
        """Example of an execute function with a single CU"""
        distributed_arguments = []
        for arg in args:
            distributed_arguments.append(self.client.scatter(arg))
            
        futures = self.client.map(func,*distributed_arguments)
        res = self.client.gather(futures)
        return res
    
    def execute_hybrid(self,func,data,kernels,*args, **kwargs):
        """Example of an execute function with multiple CU's"""        
        f = []
                
        if len(data) != len(kernels):
            raise ValueError("data and kernels don't have same dimensions.")
        futures = []
        index = 0
        for i,krnl in enumerate(kernels):
            if isinstance(krnl,dict):
                futures.append(self.client.submit(func,data[i],krnl,index+1,workers=krnl["host"]))
                index+=1
            elif isinstance(krnl,list):
                for j,k in enumerate(krnl):
                    futures.append(self.client.submit(func,data[i][j],k,index+j+1,workers=k["host"]))
                index += len(krnl)
        
        res = self.client.gather(futures)
        return res
            
            
    async def fshutdown(self):
        """WARNING: this functions forcefully kills processes on the scheduler port on each host machine.
        only use this function if your SSH Server does not support the "signal" channel request."""
        temp = [self.scheduler,*self.hosts]
        for h in temp[::-1]: 
            async with asyncssh.connect(h,22) as conn:
#                 res = await conn.run("lsof -n -i | grep "+str(self.scheduler_port) +" | awk '{system(\"kill \" $2)}'",check=True)
                res = await conn.run("pgrep -f dask | xargs kill",check=True)
                print(res.stdout,end='')

    def create_kernel(self, path:str, no_instances:int=1, batch_size:int=0, func_specs:list=[],config=None):
        """Creates a dictionary that represents a kernel.
        @param path: The path to the bitsream
        @param no_instances: If there are copied instances (default = 1)
        @param compute_unit: The id of the compute unit, this is 1 or configured based on no_instances.
        @param batch_size: The amount of data each compute unit should process.
        @param func_specs: The functions inside the kernel with their memory specifications
            A functions square_numbers(double a, double b) where a is mapped to HBM0 and b to HBM1
            is represented as: [{"square_numbers":[HBM0,HBM1]}]
        @param host: We assign each kernel to a host
        @param config: If necessary a configuration file or variable can be added.
        """ 
        kernel = {
            "path_to_kernel":path,
            "no_instances":no_instances,
            "compute_unit":1,
            "batch_size":batch_size,
            "functions":func_specs,
            "host":None,
            }    
        if config:
            kernel["config"]=config
            
        return kernel
    
    def split_data(self,dataset,kernels):
        """Split the dataset based on the amount of kernels, the number of instances and the batchsize."""
        start = 0
        self.data_split = []
        
        for krnl in kernels:
            if isinstance(krnl,list):
                group = []
                for cu in krnl:
                    group.append(dataset[start:start+cu["batch_size"]])
                    start += cu["batch_size"]
                self.data_split.append(group)
            else:
                self.data_split.append(dataset[start:start+krnl["batch_size"]])
                start += krnl["batch_size"]
                    
        return self.data_split            
    
    
    def split_kernels(self,kernels):
        """Create a separate kernel for each instance"""
        self.setup_hosts = []
        
        # Need to use slice operator to copy kernels so the insert doesn't mess up the lazy loop iterator.
        for i, krnl in enumerate(kernels[:]):
            if krnl["no_instances"]>1:
                group = []
                # Add hosts that need need to be setup for multiple compute units.
                if krnl["host"] not in self.setup_hosts:
                    self.setup_hosts.append(krnl["host"])
                
                # Increase a hosts amount of workers to the number of compute unit instances in the bitstream.
                self.worker_options[i]["n_workers"]=krnl["no_instances"]
                
                # Unpack and group a multiple compute unit kernel
                for t in range(krnl["no_instances"]):
                    new_kernel = copy.deepcopy(krnl) #need to deepcopy so we don't overwrite functions
                    new_kernel["no_instances"]=1
                    new_kernel["compute_unit"]=t+1
                    new_kernel["functions"]= krnl["functions"][t]
                    group.append(new_kernel)
                kernels[i] = group
                continue
            krnl["functions"] = krnl["functions"][0]
            krnl["no_instances"] = 1
            
        self.kernels = kernels
        return self.kernels
    
    def check_hosts(self):
        """Make sure each kernel is assigned to a valid host"""
        if len(self.hosts) == 0:
            raise ValueError("There are no hosts available, please add at least one host.")
            
    def check_kernels(self):
        for krnl in self.kernels:
            check = None
            if isinstance(krnl,dict):
                check = krnl["host"]
            elif isinstance(krnl,list):
                check = krnl[0]["host"] 
            if check not in self.hosts:
                raise ValueError(f"There is no valid host assigned to kernel {krnl}. Make sure the amount of hosts and kernels added to Octoray match.")
        
if __name__ == "__main__":
    octo = Octoray()