import os
import shutil
import subprocess
import time

src_dir = '/autofs/nas/neurobooth/data_test/100062_2022-04-06/'
dest_dir = '/autofs/nas/neurobooth/data_test_speed/100062_2022-04-06/'

if os.path.exists(dest_dir):
    shutil.rmtree(dest_dir)
else:
    os.makedirs(dest_dir, exist_ok=True)

t1 = time.time()
out = subprocess.run(["cp", src_dir, dest_dir], capture_output=True)
t2 = time.time()

time_cp = t2 - t1

shutil.rmtree(dest_dir)

t1 = time.time()
out = subprocess.run(["rsync", src_dir, dest_dir, '-arzi',
                      "--out-format=%i %n%L %t"],
                     capture_output=True)
t2 = time.time()

time_rsync = t2 - t1

print(f'Time taken by rsync is {time_rsync}')
print(f'Time taken by cp is {time_cp}')
