"""Classes that hold all data from Ocean API"""


class TokenDetail():
    """ Token Details object to save API call info """

    def __init__(self, token_id, symbol, name, is_dat,
                 is_lps, is_loan_token, token_a_id=None, token_b_id=None, created_at=None):
        self.created_at = created_at
        self.token_id = token_id
        self.symbol = symbol
        self.name = name
        self.is_dat = is_dat
        self.is_lps = is_lps
        self.is_loan_token = is_loan_token
        self.token_a_id = token_a_id
        self.token_b_id = token_b_id

    def __iter__(self):
        """ Creating an iterator to convert object attributes to tuple """
        yield self.created_at
        yield self.token_id
        yield self.symbol
        yield self.name
        yield self.is_dat
        yield self.is_lps
        yield self.is_loan_token
        yield self.token_a_id
        yield self.token_b_id

    def __str__(self):
        return f"[TokenDetail] {self.name} with symbol {self.symbol} and id {self.token_id}"


class TokenAmount():
    """ Token Holdings object to save API call info """

    def __init__(self, token_id=None, amount=None,  token_a_reserve=None, token_b_reserve=None,
                 priceratio_ab=None, priceratio_ba=None, total_liquidity_token=None,
                 total_liquidity_usd=None, apr_reward=None, apr_commission=None, volume_h24=None,
                 volume_d30=None, created_at=None):
        self.created_at = created_at
        self.token_id = token_id
        self.amount = amount
        self.token_a_reserve = token_a_reserve
        self.token_b_reserve = token_b_reserve
        self.priceratio_ab = priceratio_ab
        self.priceratio_ba = priceratio_ba
        self.total_liquidity_token = total_liquidity_token
        self.total_liquidity_usd = total_liquidity_usd
        self.apr_reward = apr_reward
        self.apr_commission = apr_commission
        self.volume_h24 = volume_h24
        self.volume_d30 = volume_d30

    def __iter__(self):
        yield self.created_at
        yield self.token_id
        yield self.amount
        yield self.token_a_reserve
        yield self.token_b_reserve
        yield self.priceratio_ab
        yield self.priceratio_ba
        yield self.total_liquidity_token
        yield self.total_liquidity_usd
        yield self.apr_reward
        yield self.apr_commission
        yield self.volume_h24
        yield self.volume_d30

    def __str__(self):
        return f"[TokenAmount] Token id {self.token_id} with amount {self.amount}"


class VaultDetail():
    """ Vault Details object to save API call info """

    def __init__(self, vault_id, colleteral_ratio, collateral_value, loan_value,
                 interest_value, created_at=None):
        self.vault_id = vault_id
        self.created_at = created_at
        self.colleteral_ratio = colleteral_ratio
        self.collateral_value = collateral_value
        self.loan_value = loan_value
        self.interest_value = interest_value

    def __iter__(self):
        """ Creating an iterator to convert object attributes to tuple """
        yield self.vault_id
        yield self.created_at
        yield self.colleteral_ratio
        yield self.collateral_value
        yield self.loan_value
        yield self.interest_value

    def __str__(self):
        return f"[VaultDetail] {self.vault_id} with colleteral ratio {self.colleteral_ratio} and collateral value {self.collateral_value}"


class VaultAmount():
    """ Vault Amounts object to save API call info """

    def __init__(self, vault_id, token_type, token_id, amount,
                 price_key, active_price, next_price, created_at=None):
        self.vault_id = vault_id
        self.created_at = created_at
        self.token_type = token_type
        self.token_id = token_id
        self.amount = amount
        self.price_key = price_key
        self.active_price = active_price
        self.next_price = next_price

    def __iter__(self):
        """ Creating an iterator to convert object attributes to tuple """
        yield self.vault_id
        yield self.created_at
        yield self.token_type
        yield self.token_id
        yield self.amount
        yield self.price_key
        yield self.active_price
        yield self.next_price

    def __str__(self):
        return f"[VaultAmount] {self.token_id} with amount {self.amount}"
