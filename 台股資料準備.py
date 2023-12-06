# %% Import packages
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text as sql_text

def pd_read_mssql_data(sql_query, server, database, server_uid, server_pwd, local=False):
    if local:
        conn = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database
    else:
        conn = 'DRIVER={};SERVER={};DATABASE={};UID={};PWD={}'.format('SQL Server', server, database, server_uid,
                                                                      server_pwd)
    quoted = quote_plus(conn)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con)
    con = engine.connect()
    df = pd.read_sql(sql_text(sql_query), con)
    con.close()
    return df

server = '10.198.213.13'
server_uid = 'ap00100773'
server_pwd = 'Wayne880807'
database = 'FinanceOther'

# %% 基本資料
stock_info = pd_read_mssql_data("""SELECT * FROM tej_twn_anprcstd  WHERE [stypenm] ='普通股' and len([coid]) = 4 and ISNUMERIC([coid])=1""", server, database, server_uid, server_pwd)
df_table_info = pd_read_mssql_data("SELECT *  FROM [FinanceOther].[dbo].[tej_twn_datatype] where [table] = 'TWN/ANPRCSTD'", server, database, server_uid, server_pwd)
stock_info = stock_info.rename(columns=dict(df_table_info[['name','cname']].values))
stock_info = stock_info[['證券碼','目前狀態','證券名稱']]
stock_info

# %% 價量資料 raw data
raw_price = pd_read_mssql_data("""SELECT * FROM tej_twn_apiprcd where [mdate]>='2020-01-01' AND [mdate]<='2023-11-30' ORDER BY [mdate],[coid]""", server, database, server_uid, server_pwd)
df_table_info = pd_read_mssql_data("SELECT *  FROM [FinanceOther].[dbo].[tej_twn_datatype] where [table] = 'TWN/APIPRCD'", server, database, server_uid, server_pwd)
raw_price = raw_price.rename(columns=dict(df_table_info[['name','cname']].values))
raw_price.rename(columns={'證券名稱':'證券碼'},inplace=True)


# %% 加權報酬指數
bm = raw_price[raw_price['證券碼'] =='IR0001 '][['資料日','收盤價']].reset_index(drop=True)
bm.to_feather('加權報酬指數.feather')

# %% 價量資料
adj_p = pd.merge(raw_price,stock_info,on='證券碼',how='inner').sort_values(by=['資料日','證券碼'], ascending=True,ignore_index=True)
adj_p['證券名稱'] = adj_p['證券碼'].str.strip() + adj_p['證券名稱']
# adj_p['開盤價'] = adj_p['開盤價'] * adj_p['調整係數']
# adj_p['最高價'] = adj_p['最高價'] * adj_p['調整係數']
# adj_p['最低價'] = adj_p['最低價'] * adj_p['調整係數']
adj_p['收盤價'] = adj_p['收盤價'] * adj_p['調整係數']
# adj_p['個股市值(元)'] = adj_p['個股市值(元)'] * adj_p['調整係數']
adj_p = adj_p[['證券名稱','資料日', '市場別', '開盤價', '最高價', '最低價', '收盤價', '成交量(千股)', '成交金額(元)', '成交筆數',
        '周轉率', '流通在外股數(千股)', '個股市值(元)', '市值比重', '成交金額比重', '現金股利率(TEJ)', '本益比(TEJ)', '股價淨值比(TEJ)', '股價營收比(TEJ)']]
adj_p.to_feather('價量資料.feather')

# %% 籌碼資料 raw data
raw_inst = pd_read_mssql_data("""SELECT * FROM tej_twn_apishract where [mdate]>='2020-01-01' AND [mdate]<='2023-11-30' ORDER BY [mdate],[coid]""", server, database, server_uid, server_pwd)
df_table_info = pd_read_mssql_data("SELECT *  FROM [FinanceOther].[dbo].[tej_twn_datatype] where [table] = 'TWN/APISHRACT'", server, database, server_uid, server_pwd)
raw_inst = raw_inst.rename(columns=dict(df_table_info[['name','cname']].values))
raw_inst.rename(columns={'證券名稱':'證券碼'},inplace=True)

# %% 籌碼資料
inst = pd.merge(raw_inst,stock_info,on='證券碼',how='inner').sort_values(by=['資料日','證券碼'], ascending=True,ignore_index=True)
inst['證券名稱'] = inst['證券碼'].str.strip() + inst['證券名稱']
inst = inst[['證券名稱','資料日', '市場別', '外資買進張數', '外資賣出張數', '外資買賣超張數', '外資買進金額(元)', '外資賣出金額(元)',
       '外資買賣超金額(元)', '外資持股率', '投信買進張數', '投信賣出張數', '投信買賣超張數', '投信買進金額(元)',
       '投信賣出金額(元)', '投信買賣超金額(元)', '投信持股率', '自營商買進張數(自行)', '自營商賣出張數(自行)',
       '自營買賣超張數(自行)', '自營商買進金額(自行)', '自營商賣出金額(自行)', '自營買賣超金額(自行)',
       '自營商買進張數(避險)', '自營商賣出張數(避險)', '自營買賣超張數(避險)', '自營商買進金額(避險)',
       '自營商賣出金額(避險)', '自營買賣超金額(避險)', '自營商持股率', '合計買進張數', '合計賣出張數', '合計買賣超張數',
       '合計買進金額(元)', '合計賣出金額(元)', '合計買賣超金額(元)', '融資買進', '融資賣出', '融資餘額',
       '融資餘額(元)', '融券買進', '融券賣出', '融券餘額', '融券餘額(元)',
       '資券比', '資券互抵', '資券互抵(元)', '借券賣出', '借券賣出(元)',  '借券餘額',
       '借券餘額(元)', '當沖成交股數(千股)', '當沖買賣占比']]
inst.to_feather('籌碼資料.feather')

# %% 月營收資料 raw data
raw_sale = pd_read_mssql_data("""SELECT * FROM tej_twn_apisale where [mdate]>='2020-01-01' AND [mdate]<='2023-11-30' ORDER BY [mdate],[coid]""", server, database, server_uid, server_pwd)
df_table_info = pd_read_mssql_data("SELECT *  FROM [FinanceOther].[dbo].[tej_twn_datatype] where [table] = 'TWN/APISALE'", server, database, server_uid, server_pwd)
raw_sale = raw_sale.rename(columns=dict(df_table_info[['name','cname']].values))
raw_sale.rename(columns={'公司':'證券碼'},inplace=True)

# %% 月營收資料
sale = pd.merge(raw_sale,stock_info,on='證券碼',how='inner').sort_values(by=['年月','證券碼'], ascending=True,ignore_index=True)
sale['證券名稱'] = sale['證券碼'].str.strip() + sale['證券名稱']
sale = sale[['證券名稱', '年月', '營收發布日', '單月營收(千元)', '去年單月營收(千元)', '單月營收成長率％', '單月營收與上月比％',
       '累計營收(千元)', '去年累計營收(千元)', '累計營收成長率％', '歷史最高單月營收(千元)',
       '與歷史最高單月營收比%', '歷史最低單月營收(千元)', '與歷史最低單月營收比%', '近12月累計營收(千元)',
       '去年近12月累計營收(千元)', '近12月累計營收成長率％', '近 3月累計營收(千元)', '去年近 3月累計營收(千元)',
       '近3月累計營收成長率％', '近3月累計營收與上月比％']]
sale.to_feather('月營收資料.feather')

# %% 財報資料 raw data (抓Q))
raw_fin = pd_read_mssql_data("""SELECT * FROM tej_twn_ainvfq1 where [mdate]>='2020-01-01' AND [mdate]<='2023-11-30' AND [key3] = 'Q' ORDER BY [mdate],[coid]""", server, database, server_uid, server_pwd)
df_table_info = pd_read_mssql_data("SELECT *  FROM [FinanceOther].[dbo].[tej_twn_datatype] where [table] = 'TWN/AINVFQ1'", server, database, server_uid, server_pwd)
raw_fin = raw_fin.rename(columns=dict(df_table_info[['name','cname']].values))
raw_fin.rename(columns={'公司':'證券碼'},inplace=True)

# %% 財報資料
fin = pd.merge(raw_fin,stock_info,on='證券碼',how='inner').sort_values(by=['年/月','證券碼'], ascending=True,ignore_index=True)
fin['證券碼'] = fin['證券碼'].str.strip() + fin['證券名稱']
fin.drop(columns=['目前狀態','證券名稱'],inplace=True)
fin.rename(columns={'證券碼':'證券名稱'},inplace=True)
fin.to_feather('季財報資料.feather')

