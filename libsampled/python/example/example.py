import sampled
import canonical

import os
import time
import psutil

def main():
    

    while True:
        la1, la5, la15 = os.getloadavg()
        cpu_times = psutil.cpu_times_percent()
        vmem = psutil.virtual_memory()
        sampled.add_sample("fleet_basic_stats", {
            "hostname": canonical.hostname(),
            "kernel": canonical.kernel(),
            "sys_name": canonical.sysname(),
            "procs_running": len(psutil.pids()),
            "load_avg_1": la1,
            "load_avg_5": la5,
            "load_avg_15": la15,
            
            "cpu_idle": getattr(cpu_times, "idle", 0),
            "cpu_iowait": getattr(cpu_times, "iowait", 0),
            "cpu_irq": getattr(cpu_times, "irq", 0),
            "cpu_nice": getattr(cpu_times, "nice", 0),
            "cpu_softirq": getattr(cpu_times, "softirq", 0),
            "cpu_system": getattr(cpu_times, "system", 0),
            "cpu_user": getattr(cpu_times, "user", 0),
            
            "mem_active": getattr(vmem, "active", 0),
            "mem_available": getattr(vmem, "available", 0),
            "mem_buffers": getattr(vmem, "buffers", 0),
            "mem_cached": getattr(vmem, "cached", 0),
            "mem_free": getattr(vmem, "free", 0),
            "mem_inactive": getattr(vmem, "inactive", 0),
            "mem_shared": getattr(vmem, "shared", 0),
            "mem_total": getattr(vmem, "total", 0),
            "mem_used": getattr(vmem, "used", 0),
            "test_int": int(1),
            "test_label_set": ["a", "b", "c"]
        })
        time.sleep(0.1)


if __name__ == '__main__':
    main()