import os
import json


while True:
    os.system("xbutil dump > power.txt")
    with open("power.txt") as f:
        data = json.load(f)
    with open("power_res.txt","a+") as f:
        f.write(data["board"]["physical"]["power"])
        f.write("\n")
    f = open("power.txt","w")
    f.truncate(0)
    f.close()







