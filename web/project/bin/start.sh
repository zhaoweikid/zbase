#!/bin/bash

#/home/xxx/python/bin/gunicorn -c ../conf/gunicorn_setting.py server:app
gunicorn -c ../conf/gunicorn_setting.py server:app
