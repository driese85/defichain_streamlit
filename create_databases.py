""" Creating PostgreSQL databases """
import sys
import os
from dotenv import load_dotenv

from utils import postgresql

# load environment variables
load_dotenv()


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

cur = con.cursor()

# Create tables
cur.execute(  # Table 1
    '''
        CREATE TABLE defichain_tokens
        (
            token_id SMALLINT UNIQUE,
            created_at TIMESTAMP WITH TIME ZONE,
            symbol VARCHAR, name VARCHAR,
            isdAT BOOL DEFAULT FALSE,
            isLPS BOOL DEFAULT FALSE,
            isLoanToken BOOL DEFAULT FALSE,
            tokenA_id SMALLINT REFERENCES defichain_tokens(token_id),
            tokenB_id SMALLINT REFERENCES defichain_tokens(token_id)
        );
    ''')
con.commit()

cur.execute(  # Table 2
    '''
        CREATE TABLE defichain_holdings
        (
            token_id SMALLINT REFERENCES defichain_tokens(token_id),
            created_at TIMESTAMP WITH TIME ZONE,
            amount REAL,
            tokenA_reserve REAL,
            tokenB_reserve REAL,
            priceratio_ab REAL,
            priceratio_ba REAL,
            total_liquidity_token REAL,
            total_liquidity_usd REAL,
            apr_reward REAL,
            apr_commission REAL,
            volume_h24 REAL,
            volume_d30 REAL
        );
    ''')
con.commit()

cur.execute(  # Table 3
    '''
        CREATE TABLE vaults
        (   id INTEGER UNIQUE GENERATED ALWAYS AS IDENTITY,
            vault_id VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE,
            collateral_ratio REAL,
            collateral_value REAL,
            loan_value REAL,
            interest_value REAL
        );
    ''')
con.commit()

cur.execute(  # Table 4
    '''
        CREATE TABLE vault_amounts
        (
            vault_id INTEGER REFERENCES vaults(id),
            created_at TIMESTAMP WITH TIME ZONE,
            token_id SMALLINT REFERENCES defichain_tokens(token_id),
            token_type VARCHAR,
            amount REAL,
            price_key VARCHAR,
            active_price REAL,
            next_price REAL
        );
    ''')
con.commit()

cur.execute(  # Table 5
    '''
        CREATE TABLE coin_prices
        (
            symbol VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE,
            pair VARCHAR,
            price REAL
        );
    ''')
con.commit()

cur.execute(  # Index 1
    '''
        CREATE INDEX idx_defichain_tokens_token_id
        ON defichain_tokens(token_id);
    ''')

cur.execute(  # Index 2
    '''
        CREATE INDEX idx_defichain_tokens_symbol
        ON defichain_tokens(symbol);
    ''')

# Save (commit) the changes
con.commit()
cur.close()
# Close the connection
con.close()
