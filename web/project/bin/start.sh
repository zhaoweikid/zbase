#!/bin/bash

/home/qfpay/python/bin/gunicorn -c setting.py server:app
