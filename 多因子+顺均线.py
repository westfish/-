#1
# 测试因子的有效性，如胜率、信息比率
# 每期都计算一次，从而因子及打分权重可以动态调整
import numpy as np
import math
from iFinDPy import *
import pandas as pd
# 初始化账户

#三行间距表示区分初始化、选因子、选股、交易策略
#两行间距表示区分各模块主要的实现步骤
#一行间距或无间距表示区分各步骤具体实现的小步骤
def initialize(account):
    # 通过过滤形成待选股票池
    get_iwencai('非停牌，非涨停，非新股,非次新股')

    # 调仓周期
    account.pp = 30

    # 调仓周期计数起点，每次调仓会变动
    account.cday = 0


    # 超低配股票池大小
    account.samt = 4

    # 因子测试期数
    account.fpn = 12

    # 调仓周期计数起点，每次调仓会变动
    account.cday = 0

    
    # account.mfactors={'valuation_pe':[0,-20],'valuation_pb':[0,-1]}
    # 设置因子的权重，-1.0表示因子越小越好,1.0表示因子越大越好
    account.mfactors = {'valuation_pe_ttm':1.0,
    'valuation_pb': -1.0,'valuation_history_peg':-1.0,'profit_roe_ths':1.0,
    'profit_net_profit_margin_on_sales':1.0,'valuation_current_market_cap':1.0,'profit_roa':1.0,'growth_opt_profit_grow_ratio':1.0}
    # 设置因子的过滤条件
    account.qf={'valuation_pe_ttm':'valuation.pe_ttm>8,valuation.pe_ttm<40',
    'valuation_pb': 'valuation.pb>0,valuation.pb<20','valuation_history_peg':'valuation.history_peg>0,valuation.history_peg<10',
    'profit_roe_ths':'profit.roe_ths>0.05','profit_net_profit_margin_on_sales':'profit.net_profit_margin_on_sales>0',
    'valuation_current_market_cap':'valuation.current_market_cap>2500000000','profit_roa':' profit.roa>0',
    'growth_opt_profit_grow_ratio':'growth.opt_profit_grow_ratio> 0.05'}
    # 本期超配股票池
    account.lastLong = {'valuation_pe': [], 'valuation_pb': []}

    # 本期低配股票池
    account.lastShort = {'valuation_pe': [], 'valuation_pb': []}

    # 因子胜率和信息比率
    account.winr = {'valuation_pe':0 , 'valuation_pb':0 }
    account.ir = {'valuation_pe':0 , 'valuation_pb':0 }


     # 股票池大小
    account.samt = 5

    # 设置因子的权重，负号表示因子越小越好,绝对值越大表示越重要
    account.mfactorswei = {'valuation_pe_ttm':1.0,
    'valuation_pb': -1.0,'valuation_history_peg':-1.0,'profit_roe_ths':1.0,
    'profit_net_profit_margin_on_sales':1.0,'valuation_current_market_cap':1.0,'profit_roa':1.0,'growth_opt_profit_grow_ratio':1.0}

    # 股票打分机制
    account.m = 5   



# 设置买卖条件，每个交易频率（日/分钟/tick）调用一次
def handle_data(account, data):
    # 距回测开始的时间
    dd = get_datetime() - account.start_date
    #判断是否到达调仓的周期
    if int(dd.days) >= account.cday:
        account.cday += account.pp


    #选因子
        #获取因子的数量
        n = len(account.mfactors)
        #要进行查询的因子
        queryc = ''
        qf=''
        #将因子池化为标准的字符串格式
        for k in account.mfactors.keys():
            queryc += k.replace('_','.',1) + ','
            qf+=account.qf[k]+','
        queryc = queryc[:-1]
        qf=qf[:-1]
        #log.info(queryc[:80])
        #log.info(queryc[80:160])
        #log.info(queryc[160:240])
        log.info('***********')
        #定义查询的内容及过滤的内容
        symb='valuation.symbol'
        q = query(
            symb,
            queryc
        ).filter(
            valuation.symbol.in_(account.iwencai_securities),
            valuation.pe_ttm>8,valuation.pe_ttm<40,profit.roe_ths>0.05,profit.net_profit_margin_on_sales>0, profit.roa>0,growth.opt_profit_grow_ratio> 0.05,valuation.current_market_cap>2500000000,valuation.history_peg>0,valuation.history_peg<10,valuation.pb>0,valuation.pb<20
        )
        #查询回测当天的满足q的数据：股票代码，各因子的值
        df = get_fundamentals(q, date=get_datetime())
        # log.info(df["valuation_symbol"])
        log.info(df)
        #根据股票的因子值排序筛选出各因子超配和低配股票池
        for k in account.mfactors.keys():
            account.lastLong[k] = []
            account.lastShort[k] = []
            
            
            tempdf = df.sort_values( by=k[k.find('_') + 1:])
            
            
            # log.info(tempdf["valuation_symbol"][tempdf.index[0]])
            # log.info(tempdf)
            if account.mfactors[k] < 0:  # 表示因子是越小越好
                account.lastLong[k] = tempdf["symbol"][tempdf.index[:account.samt]].reset_index(drop=True).tolist()
                account.lastShort[k] = tempdf["symbol"][tempdf.index[-account.samt:]].reset_index(drop=True).tolist()
            else:
                account.lastLong[k] = tempdf["symbol"][tempdf.index[-account.samt:]].reset_index(drop=True).tolist()
                account.lastShort[k] = tempdf["symbol"][tempdf.index[:account.samt]].reset_index(drop=True).tolist()
        # log.info(type(account.lastLong['valuation_pe']))


        # 计算因子胜率、信息比率
        factors={}
        i=0;
        for k in account.mfactors.keys():
            value = get_price(account.lastLong[k], None, get_datetime().strftime("%Y%m%d"), '15d', ['close'],False,None, account.fpn, is_panel=0)
            rsum1 = np.zeros((account.fpn - 1, 1))
            for stk in account.lastLong[k]:
                x = np.array(value[stk])
                rsum1 += (x[1:] - x[0:-1]) / x[0:-1]

            value = get_price(account.lastShort[k], None, get_datetime().strftime("%Y%m%d"), '15d', ['close'],False,None, account.fpn, is_panel=0)
            rsum2 = np.zeros((account.fpn - 1, 1))
            for stk in account.lastShort[k]:
                x = np.array(value[stk])
                rsum2 += (x[1:] - x[0:-1]) / x[0:-1]
            #计算各因子胜率
            account.winr[k]=(np.sum(rsum1 - rsum2 > 0) / (account.fpn - 1))
            #计算各因子信息比率
            account.ir[k]=(np.mean(rsum1 - rsum2) / np.std(rsum1 - rsum2))
            factors[k]=account.winr[k]*0.5+account.ir[k]*0.5
        #log.info(account.winr)
        #log.info(account.ir)
        sorted_fac=sorted(factors.items(), key=lambda item: item[1], reverse=True)
        log.info(sorted_fac)
        finallist=[]
        for fac in sorted_fac[:3]:
            finallist.append(fac[0])
        log.info('所选因子：')
        log.info(finallist)



#选股
        
        #finallist=['valuation_pb']
        stocks = {}
        stocks = stocks.fromkeys(df['symbol'], 0.0)

        # log.info(df.sort(columns="valuation_pe"))
        # log.info(stocks)
        n = len(df["symbol"])
        
        for i in finallist:
            tempdf = df.sort_values(by=i[i.find('_')+1:])
            # log.info(tempdf["valuation_symbol"][tempdf.index[0]])
            # log.info(tempdf)
            for pos in range(n):
                if account.mfactorswei[i] < 0:  # 表示因子是越小越好
                    stocks[tempdf["symbol"][tempdf.index[pos]]] += account.mfactorswei[i] * (account.m - int(pos * account.m / n))
                else:
                    stocks[tempdf["symbol"][tempdf.index[pos]]] += account.mfactorswei[i] * int(pos * account.m / n)
            # log.info(stocks)

        # 排名结果
        pm = sorted(stocks.items(), key=lambda item: item[1], reverse=True)
        #log.info(pm)
        #选中的股票
        newstocks = []
        for i in range(account.samt):
            newstocks.append(pm[i][0])

        log.info('所选股票：')
        log.info(newstocks)
        '''
        #每支股票买入的金额
        vtarget = account.portfolio_value / account.samt
        #浅拷贝账户的持仓情况
        currs = account.positions.copy()
        # log.info(currs)

        # 卖出没有选中的股票
        for stk in currs.keys():
            if stk not in newstocks:
                order_target(stk, 0)
                #log.info('卖出'% stk)

        # 调整选中的股票使满足目标金额
        for stk in newstocks:
            order_target_value(stk, target=vtarget)

        # log.info(pm[0][0])
        # log.info(stocks)
        '''

#双均线测略
        buy=[]
        sell=[]
        for stk in newstocks:
            # 获取股票过去20日的收盘价数据
            close = data.attribute_history(stk, ['close'], 20, '1d')
            '''
            THS_iFinDLogin('mindgo002','929789')
            start_time=get_datetime().strftime()+' 09:15:00'
            end_time=get_datetime().strftime()+' 15:15:00'
            THS_HighFrequenceSequence('300033.SZ','open;high;low;close','default',start_time,end_time)
            '''
            # 计算五日均线价格
            MA5 = close.values[-5:].mean()
            # 计算二十日均线价格
            MA20 = close.values.mean()
            # 如果五日均线大于二十日均线
            if MA5 > MA20:
                buy.append(stk)
            if MA5 < MA20 and account.positions_value > 0:
                sell.append(stk)
            
        for stk in sell:
            order_value(stk,0)
            # 记录这次买入
            log.info("买入 %s" % (stk))
            # 如果五日均线小于二十日均线，并且目前有头寸
        for stk in buy:         
            order_target(stk,account.cash/len(buy))
            # 记录这次卖出
            log.info("卖出 %s" % (stk))