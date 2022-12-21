# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 13:51:16 2022

@author: admin
"""
#Import
from datetime import date
import pandas as pd
from binance import Client
import pandas as pd




#CAKE
df =pd.read_csv("Cake_sample.csv")
print(df)



#filt = (df['Operation'] == 'Liquidity mining reward DUSD-DFI')
#df = df.loc[filt,['Date','Operation','Amount','FIAT value','FIAT currency']]
#print(df)


filt = (df['Operation'] == 'Liquidity mining reward DUSD-DFI') | (df['Operation'] == 'Liquidity mining reward dTSLA-DUSD') \
       | (df['Operation'] == 'Liquidity mining reward dVNQ-DUSD') | (df['Operation'] == 'Freezer staking bonus') \
       | (df['Operation'] == '10 years freezer reward') | (df['Operation'] == '5 years freezer reward') \
       | (df['Operation'] == 'Referral reward') | (df['Operation'] == 'Staking reward')
df = df.loc[filt,['Date','Operation','Amount','FIAT value','FIAT currency']]
df.columns = ['Date','Operation','Amount','Value','Currency']
print(df)
df = df.assign(Platform='CakeDefi')
df = df.assign(Asset='DFI')
today = date.today()

print("Today's date:", today)
today = today.strftime("%Y-%m-%d")
print(today)
df1 = df[df['Date'].str.contains(today)]
print(df)
df1 = df1.assign(Platform='CakeDefi')
df['Date'] = df['Date'].str[:10]
print(df)


#To DB
import sqlite3
import sqlalchemy
connection = sqlite3.connect('Staking.db')
cursor = connection.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS Rewards(Date DATE,Operation TEXT,Amount REAL,Value REAL,Currency TEXT, Platform TEXT, Asset TEXT)')

connection.commit()

engine = sqlalchemy.create_engine('sqlite:///Staking.db')
df.to_sql('Rewards', engine, if_exists='append', index=False)

api_key = 'xxx'
api_secret = 'xxx'

client = Client(api_key, api_secret)
#history = pd.DataFrame(client.get_asset_dividend_history())
#print(history)

hist_dic = client.get_asset_dividend_history()
print(hist_dic)
number = hist_dic['total']
print(number)
#d = {'id':[0],'tranId':[0],'asset':[0],'amount':[0],'divTime':[0],'enInfo':[0]}
#df1 = pd.DataFrame(data=d)
dic1 = hist_dic['rows'][0]
print(dic1)

#https://stackoverflow.com/questions/13784192/creating-an-empty-pandas-dataframe-then-filling-it
#new_list = list(dic1.items())
#print(new_list)

df1 = pd.DataFrame([dic1])
print(df1)
df1 = df1.iloc[0:0]
print(df1)


for val in range(number):
    print(val)
    mdf2 = hist_dic['rows'][val]
    df2 = pd.DataFrame([mdf2])
    print(df2)
    df1 = df1.append (df2, ignore_index=True, sort=False)

print(df1)
filt = (df1['enInfo'] == 'Locked Staking')

df1 = df1.loc[filt]
df1.divTime = pd.to_datetime(df1.divTime, unit ='ms')
print(df1)
#df = df.loc[:,['asset','amount','Amount','divTime','enInfo']]
#print(df1)

print(df1)
df1 = df1[['asset','amount','divTime','enInfo']]
df1 = df1.assign(Platform='Binance')
df1 = df1.assign(Currency='USD')
df1.columns = ['Asset','Amount','Date','Operation','Platform','Currency']
#df = df[['mean', '0', '1', '2', '3']]
#df1 = df1.loc[:'asset','amount','divTime','enInfo','Platform']
#df1 = df1.columns['asset','amount','date','type','platform']
df1['Date'] = df1['Date'].dt.date
print(df1)

#To DB
#cursor.execute('DROP TABLE Rewards')
cursor.execute('CREATE TABLE IF NOT EXISTS Rewards(Date DATE,Operation TEXT,Amount REAL,Currency TEXT,Platform TEXT,Asset TEXT)')

connection.commit()

engine = sqlalchemy.create_engine('sqlite:///Staking.db')
df1.to_sql('Rewards', engine, if_exists='append', index=False)

#Prices

avg_price_ada = client.get_avg_price(symbol='ADAUSDT')
avg_price_avax = client.get_avg_price(symbol='AVAXUSDT')
avg_price_sol = client.get_avg_price(symbol='SOLUSDT')
avg_price_glmr = client.get_avg_price(symbol='GLMRUSDT')
print(avg_price_ada)
print(avg_price_avax)
print(avg_price_sol)
print(avg_price_glmr)

df_ada = pd.DataFrame(avg_price_ada, index=[0])
df_avax = pd.DataFrame(avg_price_avax, index=[0])
df_sol = pd.DataFrame(avg_price_sol, index=[0])
df_glmr = pd.DataFrame(avg_price_glmr, index=[0])

df_ada = df_ada.assign(Asset='ADA')
df_avax = df_avax.assign(Asset='AVAX')
df_sol = df_sol.assign(Asset='SOL')
df_glmr = df_glmr.assign(Asset='GLMR')
df = pd.concat([df_ada, df_avax, df_sol, df_glmr])


df =df.assign(Currency='USDT')
print(df)
from datetime import date

today = date.today()
print(today)
date_dic = {'Date':str(today)}
print(date_dic)
df_date =pd.DataFrame(date_dic, index=[0])
print(df_date)
df =df.assign(Date = df_date)
print(df)

df.drop(columns=['mins'], inplace =True)
df.rename(columns={'price':'Price'}, inplace = True)
print(df)

cursor.execute('CREATE TABLE IF NOT EXISTS Prices(Price REAL,Asset TEXT,Currency TEXT,Date DATE)')

connection.commit()

engine = sqlalchemy.create_engine('sqlite:///Staking.db')
df.to_sql('Prices', engine, if_exists='append', index=False)
