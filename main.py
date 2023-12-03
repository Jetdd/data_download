'''
Author: Jet Deng
Date: 2023-12-03 18:38:58
LastEditTime: 2023-12-03 22:30:06
Description: Main File
'''

from fetch import DataFetcher


df = DataFetcher(target='future', freq='1m', dominant=True, adj=False)
df.update()