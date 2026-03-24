from fastapi import FastAPI
from pydantic import BaseModel
from enum import Enum
from dotenv import load_dotenv

import os

load_dotenv()

kube_api_url = os.getenv("KUBE_API_URL")
proxmox_api_url = os.getenv("PROXMOX_API_URL")

physical_machines_list = []
virtual_machines_list = []
kube_pods_list = []

class Level(Enum):
    WARNING = 0
    CRITICAL = 1

class Type(Enum):
    NETWORK = 0
    CPU = 1
    MEMORY = 2
    DISK_IO = 3

class Target:
    target_id: str
    target_ip: str
    cpu: float
    ram: float
    energy: float
    disk_io: float
    network: float
    cpu_load: float
    ram_load: float
    disk_io_load: float
    network_load: float 

class Maintainable_Target(Target):
    pass

class Physical_Machine(Target):
    def add_physical_machine(self):
        physical_machines_list.append(self)
    pass

class Virtual_Machine(Maintainable_Target):
    def schedual_virtual_machine(self):
        virtual_machines_list.append(self)
    pass

class Kube_Pod(Maintainable_Target):
    def schedual_kube_pod(self):
        kube_pods_list.append(self)
    pass

class Problem(BaseModel):
    level: Level
    type: Type
    value: float
    maintainable_target_type: int
    maintainable_target_id: str
    comment: str

app = FastAPI()

@app.post("/action")
def get_status(prb: Problem):
    return {"status":"ok"}
