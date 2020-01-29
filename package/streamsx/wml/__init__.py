# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to use Watson Machine Learning (WML) online scoring in topology based streaming applications.
All models which are supported by WML for online scoring can be used to score streaming data in a topology application.
The models have to be created with tools provided by WML and Cloud Pak for Data. They need to be stored in WML repository and published as an online deployment. 


Sample
++++++


"""


__version__='0.1.4'

__all__ = ['wml_online_scoring']
from streamsx.wml._wml import wml_online_scoring

