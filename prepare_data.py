import json
import numpy as np
import pandas as pd

# Всего по 200 значений в каждом столбце, чтобы было достаточно для статистики
N = 200

def read_moex(path):
    """Читаем moex"""
    with open(path, 'r', encoding='utf-8') as f:
        d = json.load(f)
    cols = d['columns'] if 'columns' in d else d['history']['columns']
    data = d['data'] if 'data' in d else d['history']['data']
    return pd.DataFrame(data, columns=cols)


# лог-доходности IMOEX
df = read_moex('data/IMOEX_full.json')
df['CLOSE'] = pd.to_numeric(df['CLOSE'])
df = df.sort_values('TRADEDATE')
prices = df['CLOSE'].values
log_returns = np.diff(np.log(prices))  # r_t = ln(P_t) - ln(P_{t-1})
x1 = log_returns[-N:]
print(f"X1: {len(x1)} лог-доходностей IMOEX")


# ставка рефенансирования ЦБ РФ помесячно
with open('data/CBR.json', 'r', encoding='utf-8') as f:
    cbr = json.load(f)
df_cbr = pd.DataFrame(cbr)
df_cbr['date'] = pd.to_datetime(df_cbr['period_start'])
df_cbr['rate'] = pd.to_numeric(df_cbr['rate'])
df_cbr = df_cbr.sort_values('date').set_index('date')
monthly = df_cbr[['rate']].resample('MS').ffill().dropna()
x2 = monthly['rate'].values[:N].astype(float)
print(f"X2: {len(x2)} помесячных ставок ЦБ")


# копейки цены SBER
df_s = read_moex('data/SBER_full.json')
df_s['CLOSE'] = pd.to_numeric(df_s['CLOSE'])
df_s = df_s.sort_values('TRADEDATE')
kopecks = np.round((df_s['CLOSE'].values * 100) % 100).astype(float)
x3 = kopecks[-N:]
print(f"X3: {len(x3)} копеек цены SBER")


# цены IMOEX бимодальное
df_pre = read_moex('data/BIIMOEX_pre.json')
df_pre['CLOSE'] = pd.to_numeric(df_pre['CLOSE'])
prices_pre = df_pre['CLOSE'].values[:N // 2]

df_post = read_moex('data/BIIMOEX_post.json')
df_post['CLOSE'] = pd.to_numeric(df_post['CLOSE'])
prices_post = df_post['CLOSE'].values[:N // 2]

x4 = np.concatenate([prices_pre, prices_post])
np.random.seed(42)
np.random.shuffle(x4)
print(f"X4: {len(x4)} цен IMOEX ({len(prices_pre)} до + {len(prices_post)} после)")


# Сохраню в том формате, в котором РГР требовалось
result = pd.DataFrame({'X1': x1, 'X2': x2, 'X3': x3, 'X4': x4})
result.to_csv('financial_data.csv', index=False)
print(result.describe().round(4))