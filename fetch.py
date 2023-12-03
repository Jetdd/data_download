'''
Author: Jet Deng
Date: 2023-12-03 16:53:10
LastEditTime: 2023-12-03 22:32:35
Description: 
'''
from config import rq_account, rq_password
import pandas as pd
import rqdatac as rq
from utils import get_save_path, handle_freq
from joblib import Parallel, delayed


class DataFetcher:
    def __init__(self, target: str, freq: str, dominant: bool, adj: bool, **kwargs) -> None:
        self.target = target  # 默认为期货 'future'，可选 'option' 'stock'
        self.freq = handle_freq(freq=freq)  # 默认为日线 'day'，可选 '1m' '5m' '15m' '30m' '60m'
        self.dominant = dominant  # 默认为True，只获取主力合约，否则获取所有合约
        self.adj = adj  # 默认为True，复权，否则不复权
        self.adjust_method = kwargs.get(
            'adjust_method', 'prev_close_ratio')  # 默认为昨收盘价比例复q权
        self.start_date = kwargs.get(
            'start_date', '20100104')  # 默认从2010年1月4日开始取数
        self.end_date = pd.to_datetime(kwargs.get('end_date', 'today')).date()  # 默认取到今天
        self.n_jobs = kwargs.get('n_jobs', 4)  # 默认4个进程(同个账号最多4个)

        if self.adj:
            self.adjust_type = kwargs.get(
                'adjust_type', 'pre')  # 默认为前复权，可选 'pre' 'none'
        else:  # 不复权时, 选择'none'即可
            self.adjust_type = 'none'

        if self.freq == 'day':  # 日线数据的时间列名为'date'
            self.time_col = 'date'
        else:  # 其他频率的数据的时间列名为'datetime'
            self.time_col = 'datetime'

    def _download(self, product_pool: list) -> None:
        """下载数据

        Args:
            product_pool (list): of list of product names, useful for Parallel computing
        """
        rq.init(rq_account, rq_password)
        for tp in product_pool:
            save_path = get_save_path(
                target=self.target, freq=self.freq, dominant=self.dominant, adj=self.adj)
            if self.dominant:
                df = rq.futures.get_dominant_price(underlying_symbols=tp,
                                                   start_date=self.start_date,
                                                   end_date=self.end_date,
                                                   frequency=self.freq,
                                                   adjust_type=self.adjust_type,
                                                   adjust_method=self.adjust_method,)
            else:  # 非主力合约默认下载全部数据
                df = rq.get_price(tp,
                                  start_date=self.start_date,
                                  end_date=self.end_date,
                                  frequency=self.freq,)

            df.to_pickle(save_path / f'{tp}.pkl')  # 存储数据
            print(f'{tp} 下载完成')

    def download(self) -> None:
        """多进程下载数据
        """
        rq.init(rq_account, rq_password)
        all_futures = rq.all_instruments(
            type="Future", date=pd.to_datetime('today'))["underlying_symbol"].unique()
        if self.n_jobs == 1:
            self._download(all_futures)
        else:
            Parallel(n_jobs=self.n_jobs)(delayed(self._download)(
                product_pool=all_futures[i::self.n_jobs]) for i in range(self.n_jobs))

    def _update(self, product_pool: list) -> None:
        """根据已有数据的最后日期更新数据
        """
        rq.init(rq_account, rq_password)
        for tp in product_pool:
            # 目前只支持主力期货数据更新
            if self. dominant:
                if self.adj:
                    # 复权数据直接从rq更新会因为前后复权因子不同导致价格错误
                    raise KeyError('暂不支持复权数据更新')
                else:
                    save_path = get_save_path(
                        target=self.target, freq=self.freq, dominant=self.dominant, adj=self.adj)  # 获取存储路径
                    df = pd.read_pickle(save_path / f'{tp}.pkl')  # 读取已有数据
                    last_date = pd.to_datetime(
                        df.reset_index()[self.time_col][-1]).date()  # 获取已有数据最后日期

                    if last_date == pd.to_datetime('today').date():  # 判断是否为最新数据
                        print(f'{tp}已是最新数据')
                        continue
                    # 获取更新数据
                    df_new = rq.get_price(tp,
                                          start_date=last_date,
                                          end_date=self.end_date,
                                          frequency=self.freq,)

                    # Sanity Check
                    # 如果更新数据为空, 则跳过
                    if len(df_new) == 0:
                        print(f'{tp}更新数据为空')
                        continue

                    df = pd.concat([df, df_new])  # 合并数据
                    df.to_pickle(save_path / f'{tp}.pkl')
                    print(f'{tp}:{last_date}-{self.end_date}更新完成')
            else:
                raise NotImplementedError('目前只支持主力合约数据更新')

    def update(self) -> None:
        """多进程更新数据
        """
        rq.init(rq_account, rq_password)
        all_futures = rq.all_instruments(
            type="Future", date=pd.to_datetime('today'))["underlying_symbol"].unique()
        if self.n_jobs == 1:
            self._update(all_futures)
        else:
            Parallel(n_jobs=self.n_jobs)(delayed(self._update)(
                product_pool=all_futures[i::self.n_jobs]) for i in range(self.n_jobs))
