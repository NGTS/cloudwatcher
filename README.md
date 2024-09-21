# Cloudwatcher

Code to interface the cloudwatcher with the NGTS monitor.

The cloudwatcher is a device installed on the roof of the office at NGTS in Paranal. Amongst other things, it measures the temperature of the sky and thus the presence of clouds.

The cloudwatcher is connected through a Moxa to the ngts-par-ocs-01 computer.

The manual of the device and further information (including the serial protocol documentation) can be found here: https://lunaticoastro.com/aag-cloud-watcher/moreinfo/

## Running/restarting the code

1. Login into par-ds
2. Connect to the ocs-01 machine with `ssh ngts-par-ocs-01`
3. List the screen sessions with `screen -ls`
4. Connect to the `cloudwatcher` screen session with `screen -xr cloudwatcher`.
    1. If the screen doesn't exist, create and join it with `screen -S cloudwatcher` and inside navigate to `/home/ops/dev/cloudwatcher`.
5. If the code was already running inside the screen, stop it with `Ctrl-C`.
6. (Re-)run the cloudwatcher code with `/usr/local/python/bin/python AAG.py`
7. Detach from the screen session by typing `Ctrl-A` followed by `D`