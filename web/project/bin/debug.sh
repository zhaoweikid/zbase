#!/bin/bash
#/home/qfpay/python/bin/python server.py debug $1
/home/qfpay/python/bin/watchmedo auto-restart -d . -p "*.py" /home/qfpay/python/bin/python server.py debug $1
