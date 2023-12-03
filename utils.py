'''
Author: Jet Deng
Date: 2023-12-03 17:00:59
LastEditTime: 2023-12-03 18:46:38
Description: 辅助函数, 用来处理rq输入的字段
'''

import datetime
import pandas as pd
from config import main_path
from pathlib import Path

def handle_freq(freq: str) -> str:
    """处理频率字段

    Args:
        freq (str): 'day', '1m', '5m', '15m', '30m', '60m'

    Returns:
        str: '1d', '1m', '5m', '15m', '30m', '60m'
    """
    if freq == 'day':
        return '1d'
    else:
        return freq

def get_save_path(target: str, freq: str, dominant: bool, adj: bool) -> Path:
    """根据输入的参数, 返回存储路径

    Args:
        target (str): Future, Option, Stock
        freq (str): 1m, 5m, 15m, 30m, 60m, day
        dominant (bool): 是否为主力合约
        adj (bool): 是否复权

    Raises:
        NotImplementedError: 目前只支持期货数据

    Returns:
        Path: 存储路径
    """
    if target == 'future':
        if dominant:
            if adj:
                save_path = main_path / 'future' / freq / 'dominant' / 'adj'
            else:
                save_path = main_path / 'future' / freq / 'dominant' / 'no_adj'
        else:
            if adj:
                save_path = main_path / 'future' / freq / 'all' / 'adj'
            else:
                save_path = main_path / 'future' / freq / 'all' / 'no_adj'
    else:
        raise NotImplementedError('目前只支持期货数据')
    save_path.mkdir(parents=True, exist_ok=True)
    return save_path
