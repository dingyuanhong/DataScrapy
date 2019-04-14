# -*- coding: utf-8 -*-
import urllib.parse

#组合参数
def Merge(keys,values):
    param = ''
    for key in keys:
        if param != '':
            param += '&'
        v = str(values[key])
        param += str(key) +'='+ urllib.parse.quote(v)
    return param;