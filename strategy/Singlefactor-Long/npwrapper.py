
import numpy as np
from numpy import log
from scipy.stats import rankdata

from datas import *

def Log(sr):
    #自然对数函数
    return np.log(sr)

def Rank(sr):
    #列-升序排序并转化成百分比
    return sr.rank(axis=1, method='min', pct=True)

def Delta(sr,period):
    #period日差分
    return sr.diff(period)

def Delay(sr,period):
    #period阶滞后项
    return sr.shift(period)

def Corr(x,y,window):
    #window日滚动相关系数
    #当一个变量值为常量，另一个变量值可变化时，此时无法计算相关度，使用0 进行填充
    r = x.rolling(window).corr(y).fillna(0)
    #同时将起始 window-1 个窗口赋值为空
    r.iloc[:(window-1), :] = None
    return r

def Cov(x,y,window):
    #window日滚动协方差
    return x.rolling(window).cov(y)

def Sum(sr,window):
    #window日滚动求和
    return sr.rolling(window).sum()

def Prod(sr,window):
    #window日滚动求乘积
    return sr.rolling(window).apply(lambda x: np.prod(x))

def Mean(sr,window):
    #window日滚动求均值
    return sr.rolling(window).mean()

def Std(sr,window):
    #window日滚动求标准差
    return sr.rolling(window).std()

def Tsrank(sr, window):
    #window日序列末尾值的顺位
    return sr.rolling(window).apply(lambda x: rankdata(x)[-1])
               
def Tsmax(sr, window):
    #window日滚动求最大值    
    return sr.rolling(window).max()

def Tsmin(sr, window):
    #window日滚动求最小值    
    return sr.rolling(window).min()

def Sign(sr):
    #符号函数
    return np.sign(sr)

def Max(sr1,sr2):
    return np.maximum(sr1, sr2)

def Min(sr1,sr2):
    return np.minimum(sr1, sr2)

def Rowmax(sr):
    return sr.max(axis=1)

def Rowmin(sr):
    return sr.min(axis=1)

def Sma(sr,n,m):
    #sma均值
    return sr.ewm(alpha=m/n, adjust=False).mean()

def Abs(sr):
    #求绝对值
    return sr.abs()

def Sequence(n):
    #生成 1~n 的等差序列
    return np.arange(1,n+1)

def Regbeta(sr,x):
    window = len(x)
    return sr.rolling(window).apply(lambda y: np.polyfit(x, y, deg=1)[0])

def Decaylinear(sr, window):  
    weights = np.array(range(1, window+1))
    sum_weights = np.sum(weights)
    return sr.rolling(window).apply(lambda x: np.sum(weights*x) / sum_weights)

def Lowday(sr,window):
    return sr.rolling(window).apply(lambda x: len(x) - x.values.argmin())

def Highday(sr,window):
    return sr.rolling(window).apply(lambda x: len(x) - x.values.argmax())

def Wma(sr,window):
    weights = np.array(range(window-1,-1, -1))
    weights = np.power(0.9,weights)
    sum_weights = np.sum(weights)

    return sr.rolling(window).apply(lambda x: np.sum(weights*x) / sum_weights)

def Count(cond,window):
    return cond.rolling(window).apply(lambda x: x.sum())

def Sumif(sr,window,cond):
    sr[~cond] = 0
    return sr.rolling(window).sum()

def Returns(df):
    return df.rolling(2).apply(lambda x: x.iloc[-1] / x.iloc[0]) - 1


