import backtrader as bt
import pandas as pd
import backtrader as bt
import datetime
from copy import deepcopy

class TestStrategy(bt.Strategy):
    params = (
        ('buy_stocks', None), # 传入各个调仓日的股票列表和相应的权重
    )
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

    def __init__(self):
        print(self.p)
        self.trade_dates = [x.date() for x in pd.to_datetime(self.p.buy_stocks['trade_date'].unique()).tolist()]
        self.buy_stock = self.p.buy_stocks # 保留调仓信息
        self.order_list = []  # 记录以往订单，在调仓日要全部取消未成交的订单
        self.buy_stocks_pre = [] # 记录上一期持仓
    
    def next(self):
        # 获取当前的回测时间点
        dt = self.datas[0].datetime.date(0)
        # 打印当前时刻的总资产
        self.log('当前总资产 %.2f' %(self.broker.getvalue()))
        # 如果是调仓日，则进行调仓操作
        if dt in self.trade_dates:
            print(f"--------------{dt} 为调仓日----------")
            #取消之前所下的没成交也未到期的订单
            if len(self.order_list) > 0:
                print("--------------- 撤销未完成的订单 -----------------")
                for od in self.order_list:
                    # 如果订单未完成，则撤销订单
                    self.cancel(od) 
                 #重置订单列表
                self.order_list = [] 

            # 提取当前调仓日的持仓列表
            buy_stocks_data = self.buy_stock.query(f"trade_date=='{dt}'")
            long_list = buy_stocks_data['sec_code'].tolist()
            print('long_list', long_list)  # 打印持仓列表

            # 对现有持仓中，调仓后不再继续持有的股票进行卖出平仓
            sell_stock = [i for i in self.buy_stocks_pre if i not in long_list]
            print('sell_stock', sell_stock)
            if sell_stock:
                print("-----------对不再持有的股票进行平仓--------------")
                for stock in sell_stock:
                    data = self.getdatabyname(stock)
                    if self.getposition(data).size > 0 :
                        od = self.close(data=data)  
                        self.order_list.append(od) # 记录卖出订单

            # 买入此次调仓的股票：多退少补原则
            print("-----------买入此次调仓期的股票--------------")
            for stock in long_list:
                w = buy_stocks_data.query(f"sec_code=='{stock}'")['weight'].iloc[0] # 提取持仓权重
                data = self.getdatabyname(stock)
                order = self.order_target_percent(data=data, target=w*0.95) # 为减少可用资金不足的情况，留 5% 的现金做备用
                self.order_list.append(order)

            self.buy_stocks_pre = long_list  # 保存此次调仓的股票列表
        
    #订单日志    
    def notify_order(self, order):
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 已被处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                    (order.ref,
                     order.executed.price,
                     order.executed.value,
                     order.executed.comm,
                     order.executed.size,
                     order.data._name))
            else:  # Sell
                self.log('SELL EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                        (order.ref,
                         order.executed.price,
                         order.executed.value,
                         order.executed.comm,
                         order.executed.size,
                         order.data._name))
                
if __name__ == "__main__":
    from dataIO import get_hs300_stocks,get_all_date_data
    import os
    __direct__ = os.path.dirname(os.path.abspath(__file__))

    year = 2019
    list_assets, df_assets = get_hs300_stocks(f'{year}-01-01')
    df_org= get_all_date_data(f'{year}-01-01', f'{year+1}-01-01', list_assets)
    df1 = df_org.rename(columns={
            "date": "datetime", 
            "asset": "sec_code"})
    df1["openinterest"] = 0
    daily_price=df1[['sec_code','datetime', "open", "close", "high", "low", "volume", 'openinterest']]
    daily_price['datetime'] = pd.to_datetime(daily_price['datetime'])

    # 以 datetime 为 index，类型为 datetime 或 date 类型，Datafeeds 默认情况下是将 index 匹配给 datetime 字段；
    daily_price = daily_price.set_index(['datetime'])
    daily_price

    from alphalens.utils import get_clean_factor_and_forward_returns
    from alphalens.tears import create_full_tear_sheet

    df_2 = df_org[['date', 'asset', "close"]]
    df_2['date'] = pd.to_datetime(df_2['date'])
    # print(df_all)

    close = df_2.pivot(index='date', columns='asset', values='close')

    alpha_name = 'Alphas191'

    # 1. 先定义需要合成的因子编号列表
    alpha_nums = [1,2]  # 比如要合并的因子编号
    factor_dfs = []

    for num in alpha_nums:
        path = os.path.join(__direct__,f'alphas/{alpha_name}/{year}/alpha{num:03d}.csv')
        df = pd.read_csv(path)
        df = df[(df['date'] >= f'{year}-01-01') & (df['date'] < f'{year+1}-01-01')]
        df['date'] = pd.to_datetime(df['date'])
        df = df.melt(id_vars=['date'], var_name='asset', value_name=f'factor{num}')
        factor_dfs.append(df)

    # 2. 把这些因子按 (date,asset) 合并到一个 DataFrame
    from functools import reduce
    df_all = reduce(lambda left, right: pd.merge(left, right, on=['date','asset']), factor_dfs)

    # 3. 对每列因子做 z-score 归一化（可以加行业/市值中性化，这里简化演示）
    for num in alpha_nums:
        col = f'factor{num}'
        df_all[col] = (df_all[col] - df_all.groupby('date')[col].transform('mean')) \
                    / df_all.groupby('date')[col].transform('std')

    # 4. 计算等权复合因子：各因子归一化后求平均
    df_all['composite_factor'] = df_all[[f'factor{num}' for num in alpha_nums]].mean(axis=1)

    # 5. 准备给 Alphalens 用的格式
    df_factor = df_all[['date','asset','composite_factor']].set_index(['date','asset']).sort_index()
    # 注意列名要叫 'factor'
    df_factor = df_factor.rename(columns={'composite_factor':'factor'})

    # 6. 取所有股票的 close 矩阵（和你原来的 close 一样）
    #    之后调用 Alphalens 生成 forward_returns
    ret0 = get_clean_factor_and_forward_returns(df_factor, close, quantiles=5)

    # 7. 拿到 5 组里第 5 组（最强势组合），按你原来那样取 top 60 支
    ret0 = ret0.reset_index()
    top_q = ret0[ret0['factor_quantile']==5]
    trade_info0 = (
        top_q[['date','asset']]
        .rename(columns={'date':'trade_date','asset':'sec_code'})
    )
    trade_info0['weight'] = 1.0 / 60
    alpha_num1 = 1


    # 读取已经计算好的因子
    alpha1 = pd.read_csv(os.path.join(__direct__,'alphas/{}/{}/alpha{:03d}.csv'.format(alpha_name, year, alpha_num1)))

    # 筛选出今年的数据，需与股票收盘日期区间一致
    alpha1 = alpha1[(alpha1['date'] >= f'{year}-01-01') & (alpha1['date'] <= f'{year+1}-01-01')]

    # 因子矩阵转换为一维数据(alphalens需要的格式)
    alpha1 = alpha1.melt(id_vars=['date'], var_name='asset', value_name='factor' )

    # date列转为日期格式
    alpha1['date'] = pd.to_datetime(alpha1['date'])
    alpha1 = alpha1[['date', 'asset', 'factor']]

    # 设置二级索引
    alpha1 = alpha1.set_index(['date', 'asset'], drop=True)
    alpha1.sort_index(inplace=True)


    ret1 = get_clean_factor_and_forward_returns(alpha1, close,quantiles=5)
    ret1 = ret1.reset_index()
    ret1 = ret1[ret1['factor_quantile'] == 5]
    # ret['week'] =  pd.to_datetime(ret['date']).dt.weekday
    # ret = ret[ret['week'] == 4]
    ret1 = ret1[['date','asset']]
    ret1['weight'] = 1/60
    trade_info1 = ret1.rename(columns={
            "date": "trade_date", 
            "asset": "sec_code"})
    trade_info1


    cerebro_ = bt.Cerebro() 

    # 按股票代码，依次循环传入数据
    for stock in daily_price['sec_code'].unique():
        # 日期对齐
        data = pd.DataFrame(index=daily_price.index.unique())
        df = daily_price.query(f"sec_code=='{stock}'")[['open','high','low','close','volume','openinterest']]
        data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
        data_.loc[:,['volume','openinterest']] = data_.loc[:,['volume','openinterest']].fillna(0)
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(method='pad')
        # data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(0)
        datafeed = bt.feeds.PandasData(dataname=data_, fromdate=datetime.datetime(year,1,1), todate=datetime.datetime(year+1,1,1))
        cerebro_.adddata(datafeed, name=stock)
        print(f"{stock} Done !") 
    cerebro = deepcopy(cerebro_)  # 深度复制已经导入数据的 cerebro_，避免重复导入数据 
    # 初始资金 100,000,000    
    cerebro.broker.setcash(100000.0) 
    # cerebro.broker.setcommission(commission=0.0015)
    # 添加策略
    cerebro.addstrategy(TestStrategy, buy_stocks=trade_info0) # 通过修改参数 buy_stocks ，使用同一策略回测不同的持仓列表

    # 添加分析指标
    # 返回年初至年末的年度收益率
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    # 计算最大回撤相关指标
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    # 计算年化收益
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns', tann=252)
    # 计算年化夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, annualize=True, riskfreerate=0) # 计算夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    # 返回收益率时序
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

    # 启动回测
    result0 = cerebro.run()
    cerebro = deepcopy(cerebro_)  # 深度复制已经导入数据的 cerebro_，避免重复导入数据 
    # 初始资金 100,000,000    
    cerebro.broker.setcash(100000.0) 
    # cerebro.broker.setcommission(commission=0.0015)
    # 添加策略
    cerebro.addstrategy(TestStrategy, buy_stocks=trade_info1) # 通过修改参数 buy_stocks ，使用同一策略回测不同的持仓列表

    # 添加分析指标
    # 返回年初至年末的年度收益率
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
    # 计算最大回撤相关指标
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    # 计算年化收益
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns', tann=252)
    # 计算年化夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, annualize=True, riskfreerate=0) # 计算夏普比率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
    # 返回收益率时序
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

    # 启动回测
    result1 = cerebro.run()