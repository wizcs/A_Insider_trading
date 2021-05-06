import tushare as ts
import pandas as pd
pro = ts.pro_api()

li = pd.read_csv('sc_list.csv')
li = li[['ts_code','industry','tingpai','label']]
inli = pd.read_excel('data/IDX_Idxtrd.xlsx')
inli['Iddate'] = inli['Iddate'].apply(lambda x:x.replace('-',''))#沪深创指数信息
ldli = pd.read_excel('data/STK_MKT_DALYR.xlsx')
ldli['TradingDate'] = ldli['TradingDate'].apply(lambda x:x.replace('-',''))#沪深创指数信息
bdli = pd.read_excel('data/STK_MKT_STKBTAL.xlsx')
bdli['TradingDate'] = ldli['TradingDate'].apply(lambda x:x.replace('-',''))#沪深创指数信息

print('读取完毕')

stock_list = pro.stock_basic(exchange='', list_status='L',fields='ts_code,name,market,industry,list_date')

def series_2_str(sr):
    '这是把股票序列变为逗号分隔tushare所需的用的'
    a = ''
    for i in sr:
        a=a+i+','
    return a

def series_2_str_2(sr):
    '这是把股票序列变为逗号分隔tushare所需的用的'
    a = ''
    b = ''
    for i in range(100):
        a=a+sr[i:i+1].values[0]+','
    for i in range(100,(len(sr)-1)):
        b = b+sr[i:i+1].values[0]+','
    return a,b

def index_list(industry,o_date):
    '获得同类型市场数据_输出股票列表'
    sl = stock_list[(stock_list['industry'] == industry) & (stock_list['list_date'].astype(int) < int(o_date))] #目前还在上市且该行业的股票列表
    return sl['ts_code']

def get_cp(tscode,tprq,time):
    '获得li数据内的详细时序数据'
    df = pro.daily(ts_code=tscode, start_date=str(tprq-10000), end_date=str(tprq))
    return df[:time]

#获得市场指数
def get_market(tscode):
    if(tscode[0] == '6'):
        return 1
    elif(tscode[0] == '3'):
        return 399006
    elif(tscode[0] == '0'):
        return 399001
def get_index_chg(market,start_date,end_date):
    inli2 = inli[(inli['Indexcd'] == market) & (inli['Iddate'].astype(int) <= int(end_date)) & (inli['Iddate'].astype(int) >= int(start_date))]
    return inli2[['Iddate','Idchg']]

#获得流动性指标

def get_ldzb(stockname,start_date,end_date):
    ldli2 = ldli[(ldli['Symbol'] == stockname) & (ldli['TradingDate'].astype(int) <= int(end_date)) & (ldli['TradingDate'].astype(int) >= int(start_date))]
    return ldli2

#获得波动性指标

def get_bdzb(stockname,start_date,end_date):
    bdli2 = bdli[(bdli['Symbol'] == stockname) & (bdli['TradingDate'].astype(int) <= int(end_date)) & (bdli['TradingDate'].astype(int) >= int(start_date))]
    return bdli2

print('正在搞',0)
t_c = li['ts_code'][0]
t_i = li['industry'][0]
t_t = li['tingpai'][0]

sample0 = get_cp(t_c,t_t,40)
sample0 = sample0.sort_values(by='trade_date')
start_d = sample0['trade_date'][0:1].values[0]
end_d = sample0['trade_date'][-1:].values[0]

st_series = index_list(t_i,start_d)
print(len(st_series))
if(len(st_series)<100):
    st_str = series_2_str(st_series)
    bbb = pro.daily(ts_code = st_str,start_date=start_d, end_date=end_d)
else:
    st_str,st_str2 = series_2_str_2(st_series)
    bbb = pro.daily(ts_code = st_str,start_date=start_d, end_date=end_d)
    bbb2 = pro.daily(ts_code = st_str2,start_date=start_d, end_date=end_d)
    bbb = bbb.append(bbb2)

#市场指数
idchg = get_index_chg(get_market(t_c),start_d,end_d)
sample0 = pd.merge(sample0,idchg,left_on = 'trade_date',right_on='Iddate' ,how='left')
#行业指数
qwe = bbb.groupby('trade_date')[['pre_close','close']].sum()
qwe['machg'] = (qwe['close']/qwe['pre_close']) - 1
sample0 = pd.merge(sample0,qwe['machg'],left_on = 'trade_date',right_index= True,how='left')
t_c_s = t_c[0:6]

#流动指标与波动指标
ldzbli = get_ldzb(int(t_c_s),start_d,end_d)
sample0 = pd.merge(sample0,ldzbli,left_on = 'trade_date',right_on='TradingDate' ,how='left')

bdzbli = get_bdzb(int(t_c_s),start_d,end_d)
sample0 = pd.merge(sample0,bdzbli,left_on = 'trade_date',right_on='TradingDate' ,how='left')

for i in li.index[1:]:
    print('正在搞',i)
    t_c = li['ts_code'][i]
    t_i = li['industry'][i]
    t_t = li['tingpai'][i]

    sample = get_cp(t_c,t_t,40)
    start_d = sample['trade_date'][0:1].values[0]
    end_d = sample['trade_date'][-1:].values[0]

    st_series = index_list(t_i,start_d)
    
    if(len(st_series)<100):
        st_str = series_2_str(st_series)
        bbb = pro.daily(ts_code = st_str,start_date=start_d, end_date=end_d)
    else:
        st_str,st_str2 = series_2_str_2(st_series)
        bbb = pro.daily(ts_code = st_str,start_date=start_d, end_date=end_d)
        bbb2 = pro.daily(ts_code = st_str2,start_date=start_d, end_date=end_d)
        bbb = bbb.append(bbb2)

    #市场指数
    idchg = get_index_chg(get_market(t_c),start_d,end_d)
    sample = pd.merge(sample,idchg,left_on = 'trade_date',right_on='Iddate' ,how='left')
    #行业指数
    qwe = bbb.groupby('trade_date')[['pre_close','close']].sum()
    qwe['machg'] = (qwe['close']/qwe['pre_close']) - 1
    sample = pd.merge(sample,qwe['machg'],left_on = 'trade_date',right_index= True,how='left')
    t_c_s = t_c[0:6]
    #流动指标与波动指标
    ldzbli = get_ldzb(int(t_c_s),start_d,end_d)
    sample = pd.merge(sample,ldzbli,left_on = 'trade_date',right_on='TradingDate' ,how='left')
    bdzbli = get_bdzb(int(t_c_s),start_d,end_d)
    sample = pd.merge(sample,bdzbli,left_on = 'trade_date',right_on='TradingDate' ,how='left')

    sample0 = pd.concat([sample0, sample], axis=0)
    print('ok')

sample0.to_csv('hatest.csv')