# -*- coding: utf-8 -*-
"""
Created on Mon May 13 03:56:22 2019

@author: Isha and nishtha
"""

import pyspark
import numpy as np
import pandas as pd

df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/Indicators.csv')
df = df.dropna() # drop rows with missing values
df = df[df.Value != 0] # drop 0 values in value column
df = df.replace("CO2 emissions from liquid fuel consumption (% of total)", "CO2 emissions (liquid fuel consumption)")
df = df.replace("CO2 emissions from solid fuel consumption (% of total)", "CO2 emissions (solid fuel consumption)")
df = df.replace("CO2 emissions from gaseous fuel consumption (% of total)", "CO2 emissions (gaseous fuel consumption)")
df = df.replace("CO2 emissions from transport (% of total fuel combustion)", "CO2 emissions (transport fuel consumption)")
df = df.replace("CO2 emissions from electricity and heat production, total (% of total fuel combustion)", "CO2 emissions (electricity and heat production)")
df = df.replace("CO2 emissions from manufacturing industries and construction (% of total fuel combustion)", "CO2 emissions (manufacturing industries and construction)")
df = df.replace("CO2 emissions from other sectors, excluding residential buildings and commercial and public services (% of total fuel combustion)", "CO2 emissions (other sectors)")
df = df.replace("CO2 emissions from residential buildings and commercial and public services (% of total fuel combustion)", "CO2 emissions (residential, commercial buildings and public services)")

df.write.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/Indicators1.csv')