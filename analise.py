
from pysus.ftp.databases.sim import SIM

sim = SIM().load()

files = sim.get_files(["CID9", "CID10"], uf=["SP"], year=[1995, 2020])
sp_cid9, sp_cid10 = files

print(sim.describe(sp_cid10))