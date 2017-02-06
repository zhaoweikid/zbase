#!/bin/bash
#/home/xxx/python/bin/python server.py debug $1
#/home/xxx/python/bin/watchmedo auto-restart -d . -p "*.py" /home/xxx/python/bin/python server.py debug $1
watchmedo auto-restart -d . -p "*.py" python server.py debug $1
