# OpenIO Collectors for python-diamond

## Description

This module provides custom collectors for python-diamond (https://github.com/python-diamond/Diamond). It specifically targets metrics provided by OpenIO-SDS, as well as the APIs to which OpenIO-SDS can bind.

## How to install (Illustrated in CentOS 7)

- Install python-diamond from the official repos

```
# yum install python-diamond
```

- Install the required dependencies (listed in the collector python file)

- Place the folder(s) containing the collector python file (e.g. openio-stable-16.04) in /usr/share/diamond/collectors/

```
# cp -r collectors/openio/openio-stable-16.04 /usr/share/diamond/collectors
```

- Place the corresponding configuration file(s) in /etc/diamond/collectors

```
# cp conf/OpenIOSDSCollector.conf.sample /etc/diamond/collectors/OpenIOSDSCollector.conf
```

- Edit the configuration file(s) to fill the required fields

- Restart diamond

```
# systemctl restart diamond
```

## TODO

- Tests
