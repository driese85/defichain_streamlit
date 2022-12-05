"""Streamlit app showcasing a DeFiChain Portfolio Dashboard powered by a PostgreSQL Database"""
import os
import time
from datetime import datetime, timedelta
import json
import pandas as pd
import pytz
import requests
import streamlit as st
import sqlalchemy

from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv
from utils import chart

# load environment variables
load_dotenv()

# set page configs
st.set_page_config(layout="centered", page_icon="↗️",
                   page_title="DefiChain Dashboard")

# update dashboard every 10 mins
st_autorefresh(interval=10 * 60 * 1000, key="dataframerefresh")

# function to create space between elements


def space(num_lines=1):
    """Adds empty lines to the Streamlit app."""
    for _ in range(num_lines):
        st.write("")


@st.cache(allow_output_mutation=True)
def connect_engine(url):
    """Initiate sqlalchemy engine"""
    sqlalchemy_engine = sqlalchemy.create_engine(url)
    return sqlalchemy_engine


# @st.cache(hash_funcs={sqlalchemy.engine.base.Engine: id})
def get_data(sql, sqlalchemy_engine):
    """Get data from PostgreSQL database server"""
    df = pd.read_sql(sql, con=sqlalchemy_engine)
    return df


connection_url = f"postgresql://{os.getenv('POSTGRESQL_USER')}:{os.getenv('POSTGRESQL_PW')}@{os.getenv('POSTGRESQL_IP')}/{os.getenv('POSTGRESQL_DB')}"
engine = connect_engine(connection_url)

# get pool_prices from ocean api
poolprices_url = "https://ocean.defichain.com/v0/mainnet/prices"

# paginate over all prices in ocean api
# dict to store results
all_pool_prices = []
# get first page
while True:
    pool_prices_page_1 = requests.get(poolprices_url, timeout=10)
    if pool_prices_page_1.status_code == 200:
        pool_prices = json.loads(pool_prices_page_1.text)
        break
    time.sleep(1)

# get rest of pages, while loop will run as long as we have a next page in the dict
_run = True
while _run:
    # merge dicts
    all_pool_prices.append(pd.json_normalize(pool_prices["data"]))
    if "page" in pool_prices:
        while True:
            pool_prices_page_n = requests.get(
                f"{poolprices_url}?next={pool_prices['page']['next']}", timeout=10)
            if pool_prices_page_n.status_code == 200:
                pool_prices = json.loads(pool_prices_page_n.text)
                break
            time.sleep(1)
    else:
        _run = False
# concatenate all df's
df_prices = pd.concat(all_pool_prices, ignore_index=True)
df_prices.set_index("price.token", inplace=True)
df_prices = df_prices[['id', 'price.aggregated.amount']]
df_prices.loc['DUSD'] = ['DUSD-USD', '1.000000']

st.title("DefiChain Dashboard")
# query to get vault token amounts
sql_vault = """
    with vault_CTE (id, collateral_ratio, collateral_value, loan_value)
    as
    (
        select id, collateral_ratio, collateral_value, loan_value
        from vaults
        order by created_at desc limit 1
    )
    select id, vault_amounts.token_id, name, symbol, token_type, amount, active_price, next_price
    from vault_CTE
    inner join vault_amounts on vault_CTE.id = vault_amounts.vault_id
    inner join defichain_tokens on vault_amounts.token_id=defichain_tokens.token_id;
    """
df_vault = get_data(sql_vault, engine)
# create active and next value columns
df_vault["active_value"] = df_vault["amount"] * df_vault["active_price"]
df_vault["next_value"] = df_vault["amount"] * df_vault["next_price"]
# group by token_type (loan or collateral) to get total sum
df_vault_details = df_vault.groupby("token_type").sum()
df_vault_details.reset_index(inplace=True)

df_vault_coll = df_vault[df_vault["token_type"]
                         == 'collateral'][["symbol", "amount"]]
# get active, next and delta collateral ratios
coll_ratio_active = df_vault_details[df_vault_details["token_type"] == 'collateral'] \
    .active_value.iloc[0] / df_vault_details[df_vault_details["token_type"] == 'loan'] \
    .active_value.iloc[0]
coll_ratio_next = df_vault_details[df_vault_details["token_type"] == 'collateral'] \
    .next_value.iloc[0] / df_vault_details[df_vault_details["token_type"] == 'loan'] \
    .next_value.iloc[0]
coll_ratio_delta = coll_ratio_next - coll_ratio_active

# get active, next and delta collateral values
active_collateral_value = df_vault_details[df_vault_details["token_type"]
                                           == 'collateral'].active_value.iloc[0]
next_collateral_value = df_vault_details[df_vault_details["token_type"]
                                         == 'collateral'].next_value.iloc[0]
collateral_delta = next_collateral_value - active_collateral_value

# get active, next and delta loan values
active_loan_value = df_vault_details[df_vault_details["token_type"]
                                     == 'loan'].active_value.iloc[0]
next_loan_value = df_vault_details[df_vault_details["token_type"]
                                   == 'loan'].next_value.iloc[0]
loan_delta = next_loan_value - active_loan_value

# query to get 24 hours worth of DFI dex prices
d = (datetime.now(pytz.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
sql_dfi_prices = f"""
    select created_at, active_price
    from vault_amounts
    where token_id = 0 and created_at > '{d}'
    order by created_at desc;
    """
df_dfi_dex_prices_24h = get_data(sql_dfi_prices, engine)

# get active, 24h ago and delta dfi prices
dfi_price_active = df_dfi_dex_prices_24h.active_price.iloc[0]
dfi_price_24h_ago = df_dfi_dex_prices_24h.active_price.iloc[-1]
delta_dfi_price = dfi_price_active / dfi_price_24h_ago * 100 - 100

# get only holdings from the last 30 minutes
d = (datetime.now(pytz.utc) - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
# query to get current token holdings in the wallet
sql_tokens = f"""
    select distinct on (dt1.token_id) dt1.token_id, dt1.symbol as token_symbol, dt1.islps, amount, dt2.symbol as tokena_symbol, dt3.symbol as tokenb_symbol, tokena_reserve, tokenb_reserve, total_liquidity_token
    from defichain_tokens dt1
    left join defichain_tokens dt2 on dt1.tokena_id=dt2.token_id
    left join defichain_tokens dt3 on dt1.tokenb_id=dt3.token_id
    inner join defichain_holdings dh on dh.token_id=dt1.token_id
    where dh.created_at > '{d}'
    order by dt1.token_id, dh.created_at desc;
    """
df_tokens = get_data(sql_tokens, engine)
df_tokens["tokena_amount"] = (
    df_tokens["amount"] / df_tokens["total_liquidity_token"]) * df_tokens["tokena_reserve"]
df_tokens["tokenb_amount"] = (
    df_tokens["amount"] / df_tokens["total_liquidity_token"]) * df_tokens["tokenb_reserve"]
df_tokena = df_tokens[["tokena_symbol", "tokena_amount"]].groupby('tokena_symbol').sum(
).reset_index().rename(columns={'tokena_symbol': 'symbol', 'tokena_amount': 'amount'})
df_tokenb = df_tokens[["tokenb_symbol", "tokenb_amount"]].groupby('tokenb_symbol').sum(
).reset_index().rename(columns={'tokenb_symbol': 'symbol', 'tokenb_amount': 'amount'})

df_token_wallet = df_tokens[df_tokens["islps"] is False][[
    "token_symbol", "amount"]].rename(columns={'token_symbol': 'symbol'})
df_token_all = pd.concat(
    [df_tokena, df_tokenb, df_token_wallet, df_vault_coll]).groupby('symbol').sum()
# merge with prices
all_tokens_with_price = pd.merge(df_token_all, df_prices, left_index=True, right_index=True)[
    ['amount', 'id', 'price.aggregated.amount']]
all_tokens_with_price.reset_index(inplace=True)
# change type to float and rename columns
all_tokens_with_price = all_tokens_with_price.astype({'price.aggregated.amount': 'float64', 'amount': 'float64'}).rename(
    columns={'index': 'Coin', 'amount': 'Amount', 'price.aggregated.amount': 'price'})
# create USD column
all_tokens_with_price['Amount (USD)'] = all_tokens_with_price["Amount"] * \
    all_tokens_with_price["price"]

# create 5 columns
col1, col2, col3, col4, col5 = st.columns(5)

# fill columns with correct formatting enabled
col1.metric("All holdings ($)", "{:,.0f}".format((all_tokens_with_price['Amount (USD)'].sum(
)-active_loan_value)), "excl. loan value", delta_color="off")
col2.metric("DFI-price (dUSD)", "{:,.2f}".format(dfi_price_active),
            "{:,.1f}% (24h %)".format(delta_dfi_price))
col3.metric("Collateral ($)", "{:,.0f}".format(active_collateral_value),
            "{:,.1f} (next)".format(collateral_delta))
col4.metric("Loans ($)", "{:,.0f}".format(active_loan_value),
            "{:,.1f} (next)".format(loan_delta))
col5.metric("Coll. Ratio", f"{(coll_ratio_active * 100).round(1)}%",
            f"{(coll_ratio_delta * 100).round(1)}% (next)")

st.header("Holdings")
col1, col2 = st.columns((45, 55))
# get all tokens sorted descending in Amount (USD)
token_sorted_price = all_tokens_with_price.set_index(
    "Coin")[["Amount", "Amount (USD)"]].sort_values("Amount (USD)", ascending=False)

col1.dataframe(token_sorted_price.style.format("{:,.2f}"))
token_sorted_price_index_reset = token_sorted_price
token_sorted_price_index_reset.reset_index(inplace=True)
chart_holdings = chart.get_holdings_chart(token_sorted_price_index_reset)
col2.altair_chart(chart_holdings, use_container_width=True)

# query to get historical token amounts
sql_historical_amount_tokens = """
    select dt1.token_id, dh.created_at, dt1.symbol as token_symbol, dt1.islps, amount, dt2.symbol as tokena_symbol, dt3.symbol as tokenb_symbol, tokena_reserve, tokenb_reserve, total_liquidity_token
    from defichain_tokens dt1
    left join defichain_tokens dt2 on dt1.tokena_id=dt2.token_id
    left join defichain_tokens dt3 on dt1.tokenb_id=dt3.token_id
    inner join defichain_holdings dh on dh.token_id=dt1.token_id
    order by dh.created_at desc;
    """
df_historical_amount_tokens = get_data(sql_historical_amount_tokens, engine)

sql_historical_vault_token = """
    select va.created_at, va.amount, dt.symbol
    from vault_amounts va
    inner join defichain_tokens dt on va.token_id=dt.token_id
    where va.token_type='collateral'
    order by created_at desc;
    """

df_historical_vault_tokens = get_data(sql_historical_vault_token, engine)
# round down to 5 minutely precision
df_historical_amount_tokens['created_at'] = df_historical_amount_tokens['created_at'].dt.floor(
    '5T')
df_historical_vault_tokens['created_at'] = df_historical_vault_tokens['created_at'].dt.floor(
    '5T')
# calculate Token A and B amount
df_historical_amount_tokens["tokena_amount"] = df_historical_amount_tokens["amount"] / \
    df_historical_amount_tokens["total_liquidity_token"] * \
    df_historical_amount_tokens["tokena_reserve"]
df_historical_amount_tokens["tokenb_amount"] = df_historical_amount_tokens["amount"] / \
    df_historical_amount_tokens["total_liquidity_token"] * \
    df_historical_amount_tokens["tokenb_reserve"]
# group tokens by datetime for timeseries chart
df_tokena_by_datetime = df_historical_amount_tokens[["created_at", "tokena_symbol", "tokena_amount"]].groupby(
    ['created_at', 'tokena_symbol']).sum().reset_index().rename(columns={'tokena_symbol': 'symbol', 'tokena_amount': 'amount'})
df_tokenb_by_datetime = df_historical_amount_tokens[["created_at", "tokenb_symbol", "tokenb_amount"]].groupby(
    ['created_at', 'tokenb_symbol']).sum().reset_index().rename(columns={'tokenb_symbol': 'symbol', 'tokenb_amount': 'amount'})
df_token_wallet_by_datetime = df_historical_amount_tokens[df_historical_amount_tokens["islps"] == False][[
    "created_at", "token_symbol", "amount"]].rename(columns={'token_symbol': 'symbol'})
df_token_wallet_by_datetime = pd.concat([df_tokena_by_datetime, df_tokenb_by_datetime, df_token_wallet_by_datetime,
                                        df_historical_vault_tokens]).groupby(['created_at', 'symbol']).sum().reset_index()
df_token_wallet_by_datetime.set_index('symbol', inplace=True)
# merge with current prices
all_tokens_with_price = pd.merge(df_token_wallet_by_datetime, df_prices, left_index=True, right_index=True)[
    ['created_at', 'amount', 'id', 'price.aggregated.amount']]
all_tokens_with_price.reset_index(inplace=True)
all_tokens_with_price = all_tokens_with_price.astype({'price.aggregated.amount': 'float64', 'amount': 'float64'}).rename(
    columns={'index': 'Coin', 'amount': 'Amount', 'price.aggregated.amount': 'price'})
all_tokens_with_price['Amount (USD)'] = (
    all_tokens_with_price["Amount"]*all_tokens_with_price["price"]).round(2)
# get list of all tokens to use in multiselect box
all_holdings_symbols = all_tokens_with_price.Coin.unique()
all_holdings_symbols = st.multiselect(
    "Choose coin to visualize", all_holdings_symbols, all_holdings_symbols)
space(1)

all_tokens_with_price = all_tokens_with_price[all_tokens_with_price.Coin.isin(
    all_holdings_symbols)]
chart_holdings = chart.get_historical_holdings_chart(all_tokens_with_price)
st.altair_chart(chart_holdings, use_container_width=True)

# display all collateral tokens
st.header('Collateral')
df_coll = df_vault[df_vault["token_type"] == 'collateral']
# get number of collateral tokens
num_of_coll_tokens = len(df_coll.index)
cols_coll = st.columns(num_of_coll_tokens)

for i, col in enumerate(cols_coll):
    col.markdown(f"<p><b>{df_coll.symbol.iloc[i]}</b><br>"
                 f"{df_coll.amount.iloc[i]}<br>"
                 f"${(df_coll.amount.iloc[i]*df_coll.active_price.iloc[i]):.2f}</p>",
                 unsafe_allow_html=True)

# display all loan tokens
st.header('Loans')

df_loans = df_vault[df_vault["token_type"] == 'loan']
num_of_loan_tokens = len(df_loans.index)
cols_loan = st.columns(num_of_loan_tokens)

for i, col in enumerate(cols_loan):
    col.markdown(f"<p><b>{df_loans.symbol.iloc[i]}</b><br>"
                 f"{df_loans.amount.iloc[i]}<br>"
                 f"${(df_loans.amount.iloc[i]*df_loans.active_price.iloc[i]):.2f}</p>",
                 unsafe_allow_html=True)

space(2)

st.header("Liquidity Mining")
# query to get reward APR%
sql_rewards = """
    select defichain_holdings.token_id, defichain_holdings.created_at,
    defichain_holdings.apr_reward, defichain_holdings.apr_commission, defichain_tokens.symbol
    from defichain_holdings
    inner join defichain_tokens on defichain_holdings.token_id=defichain_tokens.token_id
    where defichain_tokens.isLPS = True;
    """
df_rewards = get_data(sql_rewards, engine)
df_rewards["apr_total"] = (
    (df_rewards["apr_reward"] + df_rewards["apr_commission"])).round(3)


all_symbols = df_rewards.symbol.unique()
symbols = st.multiselect("Choose dToken to visualize",
                         all_symbols, all_symbols)

space(1)

df_rewards = df_rewards[df_rewards.symbol.isin(symbols)]
# create chart to display APR% for all tokens in multiselect list
chart = chart.get_apr_chart(df_rewards)
st.altair_chart(chart, use_container_width=True)

space(2)
