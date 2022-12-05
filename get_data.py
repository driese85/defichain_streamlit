""" Saving DeFiChain portfolio details to PostgreSQL database"""
import sys
import time
import json
import os
import datetime as dt
import pandas as pd
import requests
import pytz
import sqlalchemy

from dotenv import load_dotenv
from psycopg2 import OperationalError

from classes import models
from utils import postgresql

# load environment variables
load_dotenv()


def connect_engine(url):
    """ Define a connect function for PostgreSQL database server through sqlalchemy """
    sqlalchemy_engine = sqlalchemy.create_engine(url)
    return sqlalchemy_engine


# sqlalchemy connection_url
connection_url = f"postgresql://{os.getenv('POSTGRESQL_USER')}:{os.getenv('POSTGRESQL_PW')}@{os.getenv('POSTGRESQL_IP')}/{os.getenv('POSTGRESQL_DB')}"

# iniialize sqlalchemy engine
engine = connect_engine(connection_url)

# create dict with PostgreSQL connection params
conn_params_dict = {
    "user": os.getenv('POSTGRESQL_USER'),
    "password": os.getenv('POSTGRESQL_PW'),
    "host": os.getenv('POSTGRESQL_IP'),
    "port": "5432",
    "database": os.getenv('POSTGRESQL_DB')
}

# initialize connection
con = postgresql.connect(conn_params_dict)

# exit process if connect() returned error
if con is None:
    sys.exit(0)


cursor = con.cursor()


def add_token_entry(token, token_amount=None, holding_save=True):
    """ define function to add defichain_token holdings to psql database """
    try:
        postgresql_insert_with_param_tokens = '''
                                                    INSERT INTO defichain_tokens
                                                    (
                                                        created_at,
                                                        token_id,
                                                        symbol,
                                                        name,
                                                        isDAT,
                                                        isLPS,
                                                        isLoanToken,
                                                        tokenA_id,
                                                        tokenB_id
                                                    )
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                    ON CONFLICT ON CONSTRAINT defichain_tokens_token_id_key
                                                    DO NOTHING;
                                              '''

        postgresql_insert_with_param_amounts = '''
                                                    INSERT INTO defichain_holdings
                                                    (
                                                        created_at,
                                                        token_id,
                                                        amount,
                                                        tokenA_reserve,
                                                        tokenB_reserve,
                                                        priceratio_ab,
                                                        priceratio_ba,
                                                        total_liquidity_token,
                                                        total_liquidity_usd,
                                                        apr_reward,
                                                        apr_commission,
                                                        volume_h24,
                                                        volume_d30
                                                    )
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                '''

        timezone = pytz.timezone('Europe/Amsterdam')
        token.created_at = dt.datetime.now(timezone)
        # transform token object to tuple
        data_tuple_tokens = tuple(token)
        cursor.execute(postgresql_insert_with_param_tokens, data_tuple_tokens)

        # when saving vault tokens we don't have token holdings to save --> holding_save == False
        if holding_save:
            token_amount.created_at = dt.datetime.now(timezone)
            data_tuple_amounts = tuple(token_amount)
            cursor.execute(postgresql_insert_with_param_amounts,
                           data_tuple_amounts)
        return None
    except OperationalError as err:
        postgresql.show_psycopg2_exception(err)
        return None
    except Exception as err:
        print(f"Error occured in postgresql transaction: \n{err}")
        return None


def add_vault_entry(vault):
    """ define function to add vault details to psql database """
    try:
        postgresql_insert_with_param_vault = '''
                                                INSERT INTO vaults
                                                (
                                                    vault_id,
                                                    created_at,
                                                    collateral_ratio,
                                                    collateral_value,
                                                    loan_value,
                                                    interest_value
                                                )
                                                VALUES (%s, %s, %s, %s, %s, %s) returning id;
                                            '''

        timezone = pytz.timezone('Europe/Amsterdam')
        vault.created_at = dt.datetime.now(timezone)
        data_tuple_vaults = tuple(vault)
        cursor.execute(postgresql_insert_with_param_vault, data_tuple_vaults)
        # return vault_id
        return cursor.fetchone()[0]
    except OperationalError as err:
        postgresql.show_psycopg2_exception(err)
        return None
    except Exception as err:
        print(f"Error occured in postgresql transaction: \n{err}")
        return None


def add_vault_amount_entry(vault_amount):
    """ define function to add amounts to psql database """
    try:
        # insert developer detail
        postgresql_insert_with_param_vault_amount = '''
                                                        INSERT INTO vault_amounts
                                                        (
                                                            vault_id,
                                                            created_at,
                                                            token_type,
                                                            token_id,
                                                            amount,
                                                            price_key,
                                                            active_price,
                                                            next_price
                                                        )
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                                    '''

        timezone = pytz.timezone('Europe/Amsterdam')
        vault_amount.created_at = dt.datetime.now(timezone)
        data_tuple_vault_amount = tuple(vault_amount)
        cursor.execute(postgresql_insert_with_param_vault_amount,
                       data_tuple_vault_amount)
        return None

    except OperationalError as err:
        postgresql.show_psycopg2_exception(err)
        return None
    except Exception as err:
        print(f"Error occured in postgresql transaction: \n{err}")
        return None


def parse_token_data(req):
    """ Parse token data retrieved from API call to Ocean API"""
    data = json.loads(req.text)['data']
    # loop over all tokens in data
    for token in data:
        # initiate TokenDetail and TokenAmount objects with data retrieved from Ocean API
        token_instance = models.TokenDetail(token["id"], token["symbol"], token["name"], token["isDAT"], token["isLPS"],
                                            token["isLoanToken"])
        token_amount_instance = models.TokenAmount(
            token["id"], token["amount"])
        # if token is a Liquidity Pool token, get pool info
        if token["isLPS"] is True:
            time.sleep(0.3)
            # get pool info from Ocean API
            url_poolpair = f"https://Ocean.defichain.com/v0/mainnet/poolpairs/{token['id']}"
            while True:
                r_poolpair = requests.get(url_poolpair, timeout=10)

                if r_poolpair.status_code == 200:
                    poolpair = json.loads(r_poolpair.text)['data']
                    token_amount_instance.token_a_reserve = poolpair["tokenA"]["reserve"]
                    token_amount_instance.token_b_reserve = poolpair["tokenB"]["reserve"]
                    token_amount_instance.priceratio_ab = poolpair["priceRatio"]["ab"]
                    token_amount_instance.priceratio_ba = poolpair["priceRatio"]["ba"]
                    token_amount_instance.total_liquidity_token = poolpair["totalLiquidity"]["token"]
                    token_amount_instance.total_liquidity_usd = poolpair["totalLiquidity"]["usd"]
                    token_amount_instance.apr_reward = poolpair["apr"]["reward"]
                    token_amount_instance.apr_commission = poolpair["apr"]["commission"]
                    token_amount_instance.volume_h24 = poolpair["volume"]["h24"]
                    token_amount_instance.volume_d30 = poolpair["volume"]["d30"]
                    token_instance.token_a_id = poolpair["tokenA"]["id"]
                    token_instance.token_b_id = poolpair["tokenB"]["id"]
                    break
                time.sleep(1)
        # save token info to database
        add_token_entry(token_instance, token_amount_instance)
    return print("All done!")


def parse_vault_data(req):
    """ Parse vault data retrieved from API call to Ocean API"""
    data = json.loads(req.text)['data']
    vault_instance = models.VaultDetail(data["vaultId"], data["informativeRatio"],
                                        data["collateralValue"], data["loanValue"],
                                        data["interestValue"])
    vault_id = add_vault_entry(vault_instance)

    for collateral in data["collateralAmounts"]:
        token_id = collateral["id"]
        symbol = collateral["symbol"]
        name = collateral["name"]
        is_dat = True
        is_lps = False
        is_loan_token = False
        token = models.TokenDetail(token_id, symbol, name,
                                   is_dat, is_lps, is_loan_token)
        token_amount_instance = models.TokenAmount()
        add_token_entry(token, token_amount_instance, holding_save=False)
        con.commit()
        token_type = 'collateral'
        amount = collateral["amount"]
        if collateral["id"] == '15':
            price_key, active_price, next_price = "DUSD-USD", 1, 1
        else:
            price_key = collateral["activePrice"]["key"]
            active_price = collateral["activePrice"]["active"]["amount"]
            next_price = collateral["activePrice"]["next"]["amount"]
        vault_amount_instance = models.VaultAmount(
            vault_id, token_type, token_id, amount, price_key, active_price, next_price)
        add_vault_amount_entry(vault_amount_instance)

    for loan in data["loanAmounts"]:
        token_type = 'loan'
        token_id = loan["id"]
        amount = loan["amount"]
        if token_id == "15":
            price_key, active_price, next_price = "DUSD-USD", 1, 1
        else:
            price_key = loan["activePrice"]["key"]
            active_price = loan["activePrice"]["active"]["amount"]
            next_price = loan["activePrice"]["next"]["amount"]
        vault_amount_instance = models.VaultAmount(
            vault_id, token_type, token_id, amount, price_key, active_price, next_price)
        add_vault_amount_entry(vault_amount_instance)
    return print("All done!")


def get_token_prices():
    """ Get all current token prices with API call to Ocean """
    try:
        # get pool_prices from Ocean api
        poolprices_url = "https://Ocean.defichain.com/v0/mainnet/prices"

        # paginate over all prices in Ocean api
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
        # set df index to token
        df_prices.set_index("price.token", inplace=True)
        # condense df
        df_prices = df_prices[['id', 'price.aggregated.amount']]
        # set price of DUSD-USD to 1.0
        df_prices.loc['DUSD'] = ['DUSD-USD', '1.000000']
        timezone = pytz.timezone('Europe/Amsterdam')
        df_prices['created_at'] = dt.datetime.now(timezone)
        # rename column names to match database column names
        df_prices.rename(
            columns={'price.aggregated.amount': 'price', 'id': 'pair'}, inplace=True)
        # save df to database inside pandas with the to_sql method
        df_prices.to_sql('coin_prices', con=engine,
                         if_exists='append', index=True, index_label='symbol')
    except Exception as err:
        print(f"Error occured in postgresql transaction: \n{err}")
        return None

    return None


def main():
    """Retrieving token data from Ocean API and saving to PostgreSQL database"""
    # DeFiChain address to retrieve tokens from
    address = "df1q9qtltnhkn3f5wnjmjddq02tw32lfk0tuu9zl8h"
    url_tokens = f"https://Ocean.defichain.com/v0/mainnet/address/{address}/tokens"

    while True:
        req = requests.get(url_tokens, timeout=10)
        if req.status_code == 200:
            # parse token data retrieved from Ocean API
            parse_token_data(req)
            break
        time.sleep(1)

    # DeFiChain Vault id to retrieve vault details from
    vault_id = "f8f7333cb0d81dd4293c49ce2101328ecf297678ec442f7a7131f2ed088f8601"

    url_vault = f"https://Ocean.defichain.com/v0/mainnet/loans/vaults/{vault_id}"

    while True:
        req_vault = requests.get(url_vault, timeout=10)
        if req_vault.status_code == 200:
            # parse vault data retrieved from Ocean API
            parse_vault_data(req_vault)
            break
        time.sleep(1)

    # get token prices from Ocean API
    get_token_prices()

    # Save (commit) the changes
    con.commit()
    cursor.close()
    # Close the connection
    con.close()


if __name__ == "__main__":
    main()
