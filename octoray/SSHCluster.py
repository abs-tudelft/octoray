from __future__ import annotations

import copy
import logging
import sys
import warnings
import weakref
from json import dumps

import dask
import dask.config

from distributed.deploy.ssh import Process, Scheduler, logger
from distributed.deploy.spec import ProcessInterface, SpecCluster



class MyWorker(Process):
    """A Remote Dask Worker controled by SSH
    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    address: str
        The hostname where we should run this worker
    worker_class: str
        The python class to use to create the worker.
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this worker.
    kwargs: dict
        These will be passed through the dask-worker CLI to the
        dask.distributed.Worker class
    """

    def __init__(
        self,
        scheduler: str,
        address: str,
        connect_options: dict,
        kwargs: dict,
        worker_module="deprecated",
        worker_class="distributed.Nanny",
        remote_python=None,
        loop=None,
        name=None,
    ):
        super().__init__()

        if worker_module != "deprecated":
            raise ValueError(
                "worker_module has been deprecated in favor of worker_class. "
                "Please specify a Python class rather than a CLI module."
            )

        self.address = address
        self.scheduler = scheduler
        self.worker_class = worker_class
        self.connect_options = connect_options
        self.kwargs = copy.copy(kwargs)
        self.name = name
        self.remote_python = remote_python
        if kwargs.get("nprocs") is not None and kwargs.get("n_workers") is not None:
            raise ValueError(
                "Both nprocs and n_workers were specified. Use n_workers only."
            )
        elif kwargs.get("nprocs") is not None:
            warnings.warn(
                "The nprocs argument will be removed in a future release. It has been "
                "renamed to n_workers.",
                FutureWarning,
            )
            self.n_workers = self.kwargs.pop("nprocs", 1)
        else:
            self.n_workers = self.kwargs.pop("n_workers", 1)

    @property
    def nprocs(self):
        warnings.warn(
            "The nprocs attribute will be removed in a future release. It has been "
            "renamed to n_workers.",
            FutureWarning,
        )
        return self.n_workers

    @nprocs.setter
    def nprocs(self, value):
        warnings.warn(
            "The nprocs attribute will be removed in a future release. It has been "
            "renamed to n_workers.",
            FutureWarning,
        )
        self.n_workers = value

    async def start(self):
        try:
            import asyncssh  # import now to avoid adding to module startup time
        except ImportError:
            raise ImportError(
                "Dask's SSHCluster requires the `asyncssh` package to be installed. "
                "Please install it using pip or conda."
            )
       
        xrt = ""
        if "xrt" in self.connect_options:
            #explicitly copy the dict and remove xrt
            connect_options = dict(self.connect_options)
            del connect_options["xrt"]
            xrt = ("source " + self.connect_options["xrt"] + " && ")
        
        dir = ""
        if "dir" in connect_options:
            #explicitly copy the dict and remove xrt
            del connect_options["dir"]
            dir = ("cd " + self.connect_options["dir"] + " && ")
                   
        self.connection = await asyncssh.connect(self.address, **connect_options)

        result = await self.connection.run("uname")
        if result.exit_status == 0:
            set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
                dask.config.serialize(dask.config.global_config)
            )
        else:
            result = await self.connection.run("cmd /c ver")
            if result.exit_status == 0:
                set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                    dask.config.serialize(dask.config.global_config)
                )
            else:
                raise Exception(
                    "Worker failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
                )

        if not self.remote_python:
            self.remote_python = sys.executable

        cmd = " ".join(
            [
                set_env,
                self.remote_python,
                "-m",
                "distributed.cli.dask_spec",
                self.scheduler,
                "--spec",
                "'%s'"
                % dumps(
                    {
                        i: {
                            "cls": self.worker_class,
                            "opts": {
                                **self.kwargs,
                            },
                        }
                        for i in range(self.n_workers)
                    }
                ),
            ]
        )

        
        cmd = dir +  xrt + cmd
        self.proc = await self.connection.create_process(cmd)

        # We watch stderr in order to get the address, then we return
        started_workers = 0
        while started_workers < self.n_workers:
            line = await self.proc.stderr.readline()
            if not line.strip():
                raise Exception("Worker failed to start")
            logger.info(line.strip())
            if "worker at" in line:
                started_workers += 1
        logger.debug("%s", line)
        await super().start()
        

old_cluster_kwargs = {
    "scheduler_addr",
    "scheduler_port",
    "worker_addrs",
    "nthreads",
    "nprocs",
    "n_workers",
    "ssh_username",
    "ssh_port",
    "ssh_private_key",
    "nohost",
    "logdir",
    "remote_python",
    "memory_limit",
    "worker_port",
    "nanny_port",
    "remote_dask_worker",
}

class OctoSSHCluster():  
   
    """Deploy a Dask cluster using SSH
    The SSHCluster function deploys a Dask Scheduler and Workers for you on a
    set of machine addresses that you provide.  The first address will be used
    for the scheduler while the rest will be used for the workers (feel free to
    repeat the first hostname if you want to have the scheduler and worker
    co-habitate one machine.)
    You may configure the scheduler and workers by passing
    ``scheduler_options`` and ``worker_options`` dictionary keywords.  See the
    ``dask.distributed.Scheduler`` and ``dask.distributed.Worker`` classes for
    details on the available options, but the defaults should work in most
    situations.
    You may configure your use of SSH itself using the ``connect_options``
    keyword, which passes values to the ``asyncssh.connect`` function.  For
    more information on these see the documentation for the ``asyncssh``
    library https://asyncssh.readthedocs.io .
    Parameters
    ----------
    hosts : list[str]
        List of hostnames or addresses on which to launch our cluster.
        The first will be used for the scheduler and the rest for workers.
    connect_options : dict or list of dict, optional
        Keywords to pass through to :func:`asyncssh.connect`.
        This could include things such as ``port``, ``username``, ``password``
        or ``known_hosts``. See docs for :func:`asyncssh.connect` and
        :class:`asyncssh.SSHClientConnectionOptions` for full information.
        If a list it must have the same length as ``hosts``.
    worker_options : dict, optional
        Keywords to pass on to workers.
    scheduler_options : dict, optional
        Keywords to pass on to scheduler.
    worker_class: str
        The python class to use to create the worker(s).
    remote_python : str or list of str, optional
        Path to Python on remote nodes.
    Examples
    --------
    Create a cluster with one worker:
    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(["localhost", "localhost"])
    >>> client = Client(cluster)
    Create a cluster with three workers, each with two threads
    and host the dashdoard on port 8797:
    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "localhost", "localhost", "localhost"],
    ...     connect_options={"known_hosts": None},
    ...     worker_options={"nthreads": 2},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"}
    ... )
    >>> client = Client(cluster)
    Create a cluster with two workers on each host:
    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "localhost", "localhost", "localhost"],
    ...     connect_options={"known_hosts": None},
    ...     worker_options={"nthreads": 2, "n_workers": 2},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"}
    ... )
    >>> client = Client(cluster)
    An example using a different worker class, in particular the
    ``CUDAWorker`` from the ``dask-cuda`` project:
    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "hostwithgpus", "anothergpuhost"],
    ...     connect_options={"known_hosts": None},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"},
    ...     worker_class="dask_cuda.CUDAWorker")
    >>> client = Client(cluster)
    See Also
    --------
    dask.distributed.Scheduler
    dask.distributed.Worker
    asyncssh.connect
    """
    
    
    def __init__(self,
        hosts: list[str] | None = None,
        connect_options: dict | list[dict] = {},
        worker_options: dict = {},
        scheduler_options: dict = {},
        worker_module: str = "deprecated",
        worker_class: str = "distributed.Nanny",
        remote_python: str | list[str] | None = None,
        **kwargs,
    ):
        self.hosts = hosts
        self.connect_options = connect_options
        self.worker_options = worker_options
        self.scheduler_options = scheduler_options
        self.worker_module = worker_module
        self.worker_class = worker_class
        self.remote_python = remote_python
        self.kwargs = kwargs
        
    def create_cluster(self):
        if self.worker_module != "deprecated":
            raise ValueError(
                "worker_module has been deprecated in favor of worker_class. "
                "Please specify a Python class rather than a CLI module."
            )

        if set(self.kwargs) & old_cluster_kwargs:
            from distributed.deploy.old_ssh import SSHCluster as OldSSHCluster

            warnings.warn(
                "Note that the SSHCluster API has been replaced.  "
                "We're routing you to the older implementation.  "
                "This will be removed in the future"
            )
            self.kwargs.setdefault("worker_addrs", self.hosts)
            return OldSSHCluster(**self.kwargs)

        if not self.hosts:
            raise ValueError(
                f"`hosts` must be a non empty list, value {repr(self.hosts)!r} found."
            )
        if isinstance(self.connect_options, list) and len(self.connect_options) != len(self.hosts):
            raise RuntimeError(
                "When specifying a list of connect_options you must provide a "
                "dictionary for each address."
            )

        if isinstance(self.remote_python, list) and len(self.remote_python) != len(self.hosts):
            raise RuntimeError(
                "When specifying a list of remote_python you must provide a "
                "path for each address."
            )
        self.workers = {
            i: {
                "cls": MyWorker,
                "options": {
                    "address": host,
                    "connect_options": self.connect_options
                    if isinstance(self.connect_options, dict)
                    else self.connect_options[i],
                    "kwargs": self.worker_options
                    if isinstance(self.worker_options,dict)
                    else self.worker_options[i],
                    "worker_class": self.worker_class,
                    "remote_python": self.remote_python[i]
                    if isinstance(self.remote_python, list)
                    else self.remote_python,
                },
            }
            for i, host in enumerate(self.hosts[1:])
        }
        #the scheduler doesn't need to source the xrt environment or change to the correct dir.
        connect_options_scheduler = []
        if isinstance(self.connect_options,list):
            for c in self.connect_options:
                connect_options_scheduler.append(dict(c))
                for arg in ["xrt","dir"]:
                    if arg in c:
                        del connect_options_scheduler[-1][arg]
        else:
            connect_options_scheduler = dict(self.connect_options)
            for arg in ["xrt","dir"]:
                    if arg in self.connect_options:
                        del connect_options_scheduler[arg]

        self.scheduler = {
            "cls": Scheduler,
            "options": {
                "address": self.hosts[0],
                "connect_options": connect_options_scheduler
                if isinstance(connect_options_scheduler, dict)
                else connect_options_scheduler[0],
                "kwargs": self.scheduler_options,
                "remote_python": self.remote_python[0]
                if isinstance(self.remote_python, list)
                else self.remote_python,
            },
        }
        
        return SpecCluster(self.workers, self.scheduler, name="SSHCluster", **self.kwargs)
    

