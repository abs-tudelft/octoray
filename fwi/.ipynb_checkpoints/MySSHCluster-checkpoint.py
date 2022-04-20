from dask.distributed.deploy.old_ssh import * 

class MySSHCluster(SSHCluster):
    def __init__(
        self,
        scheduler_addr,
        scheduler_port,
        workers,
        nthreads=0,
        ssh_username=None,
        ssh_port=22,
        ssh_private_key=None,
        nohost=False,
        logdir=None,
        remote_python=None,
        memory_limit=None,
        worker_port=None,
        nanny_port=None,
        remote_dask_worker= "distributed.cli.dask_worker",
        local_directory=None,
        **kwargs,
    ):
        self.scheduler_addr = scheduler_addr
        self.scheduler_port = scheduler_port
        self.nthreads = nthreads
        nprocs = kwargs.pop("nprocs", None)
        if kwargs:
            raise TypeError(
                f"__init__() got an unexpected keyword argument {', '.join(kwargs.keys())}"
            )
        elif nprocs is not None:
            warnings.warn(
                "The nprocs argument will be removed in a future release. It has been "
                "renamed to n_workers.",
                FutureWarning,
            )

        self.ssh_username = ssh_username
        self.ssh_port = ssh_port
        self.ssh_private_key = ssh_private_key

        self.nohost = nohost

        self.remote_python = remote_python

        self.memory_limit = memory_limit
        self.worker_port = worker_port
        self.nanny_port = nanny_port
        self.remote_dask_worker = remote_dask_worker
        self.local_directory = local_directory

        # Generate a universal timestamp to use for log files
        import datetime

        if logdir is not None:
            logdir = os.path.join(
                logdir,
                "dask-ssh_" + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
            )
            print(
                bcolors.WARNING + "Output will be redirected to logfiles "
                'stored locally on individual worker nodes under "{logdir}".'.format(
                    logdir=logdir
                )
                + bcolors.ENDC
            )
        self.logdir = logdir

        # Keep track of all running threads
        self.threads = []

        # Start the scheduler node
        self.scheduler = start_scheduler(
            logdir,
            scheduler_addr,
            scheduler_port,
            ssh_username,
            ssh_port,
            ssh_private_key,
            remote_python,
        )

        # Start worker nodes
        self.workers = []
        for worker in (workers):
            self.add_worker(worker)
        
    def add_worker(self, worker):
        self.workers.append(
            start_my_worker(
                self.logdir,
                self.scheduler_addr,
                self.scheduler_port,
                worker["addr"],
                self.nthreads,
                worker["n_workers"],
                self.ssh_username,
                self.ssh_port,
                self.ssh_private_key,
                self.nohost,
                self.memory_limit,
                self.worker_port,
                self.nanny_port,
                self.remote_python,
                self.remote_dask_worker,
                self.local_directory,
            )
        )
        

def start_my_worker(
    logdir,
    scheduler_addr,
    scheduler_port,
    worker_addr,
    nthreads,
    n_workers,
    ssh_username,
    ssh_port,
    ssh_private_key,
    nohost,
    memory_limit,
    worker_port,
    nanny_port,
    remote_python=None,
    remote_dask_worker="distributed.cli.dask_worker",
    local_directory=None,
):
# "dask-worker tcp://10.1.212.126:8786 --preload pynqimport.py --memory-limit 0 --no-nanny --nthreads 1")
    cmd = (
        "{python} -m {remote_dask_worker} "
        "{scheduler_addr}:{scheduler_port} "
        "--preload pynqimport.py "
        "--nthreads {nthreads}" + (" --nworkers {n_workers}" if n_workers != 1 else "")
    )

    if not nohost:
        cmd += " --host {worker_addr}"

    if memory_limit:
        cmd += " --memory-limit {memory_limit}"

    if worker_port:
        cmd += " --worker-port {worker_port}"

    if nanny_port:
        cmd += " --nanny-port {nanny_port}"

    cmd = cmd.format(
        python=remote_python or sys.executable,
        remote_dask_worker=remote_dask_worker,
        scheduler_addr=scheduler_addr,
        scheduler_port=scheduler_port,
        worker_addr=worker_addr,
        nthreads=nthreads,
        n_workers=n_workers,
        memory_limit=memory_limit,
        worker_port=worker_port,
        nanny_port=nanny_port,
    )

    if local_directory is not None:
        cmd += " --local-directory {local_directory}".format(
            local_directory=local_directory
        )

    # Optionally redirect stdout and stderr to a logfile
    if logdir is not None:
        cmd = f"mkdir -p {logdir} && {cmd}"
        cmd += "&> {logdir}/dask_scheduler_{addr}.log".format(
            addr=worker_addr, logdir=logdir
        )

    label = f"worker {worker_addr}"

    # Create a command dictionary, which contains everything we need to run and
    # interact with this command.
    input_queue = Queue()
    output_queue = Queue()
    cmd_dict = {
        "cmd": cmd,
        "label": label,
        "address": worker_addr,
        "input_queue": input_queue,
        "output_queue": output_queue,
        "ssh_username": ssh_username,
        "ssh_port": ssh_port,
        "ssh_private_key": ssh_private_key,
    }

    # Start the thread
    thread = Thread(target=async_ssh, args=[cmd_dict])
    thread.daemon = False
    thread.start()
    return merge(cmd_dict, {"thread": thread})


