+ 关闭地址空间随机化

```
echo 0 > /proc/sys/kernel/randomize_va_space
```

+ 关闭CPU动态频率调整

```
echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

+ 关闭超线程

https://github.com/andreas-abel/nanoBench

里面有一个disable-HT.sh

+ 关闭睿频

```
echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
```

