import json
from decimal import Decimal

from flask import current_app, g
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware 
import decimal
import requests

from .. import events
from ..config import config
from ..models import Accounts, Settings, db
from ..token import Token, Coin
from ..logging import logger
from . import api
from app import create_app
from ..unlock_acc import get_account_password

w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)


app = create_app()
app.app_context().push()

@api.post("/generate-address")
def generate_new_address():    
    new_address = w3.geth.personal.new_account(get_account_password())
    crypto_str = str(g.symbol)
    with app.app_context():
        db.session.add(Accounts(address = new_address, 
                                            crypto = crypto_str,
                                            amount = 0,
                                            ))
        
        db.session.commit()
        db.session.close()
        db.session.remove()
        db.engine.dispose()
    logger.info(f'Added new address to DB')
    return {'status': 'success', 'address': new_address}

@api.post('/balance')
def get_balance():
    crypto_str = str(g.symbol)   
    if crypto_str == config["COIN_SYMBOL"]:
        inst = Coin(config["COIN_SYMBOL"])
        balance = inst.get_fee_deposit_coin_balance()
    else:
        if crypto_str in config['TOKENS'][config["CURRENT_BNB_NETWORK"]].keys():
            token_instance = Token(crypto_str)
            balance = token_instance.get_fee_deposit_token_balance()
        else:
            return {'status': 'error', 'msg': 'token is not defined in config'}
    return {'status': 'success', 'balance': balance}

@api.post('/status')
def get_status():
    with app.app_context():
        pd = Settings.query.filter_by(name = 'last_block').first()
    
    last_checked_block_number = int(pd.value)
    block =  w3.eth.get_block(w3.toHex(last_checked_block_number))
    return {'status': 'success', 'last_block_timestamp': block['timestamp']}

@api.post('/transaction/<txid>')
def get_transaction(txid):
    list_accounts = w3.geth.personal.list_accounts()
    if g.symbol == config["COIN_SYMBOL"]:
        try:
            transaction = w3.eth.get_transaction(txid)
            if (transaction['to'] in list_accounts) and (transaction['from'] in list_accounts):
                address = transaction["from"]
                category = 'internal'
            elif transaction['to'] in list_accounts:
                address = transaction["to"]
                category = 'receive'
            elif transaction['from'] in list_accounts:                
                address = transaction["from"]
                category = 'send'
            else:
                return {'status': 'error', 'msg': 'txid is not related to any known address'}
            amount = w3.fromWei(transaction["value"], "ether") 
            confirmations = int(w3.eth.blockNumber) - int(transaction["blockNumber"])
        except Exception as e:
            return {f'status': 'error', 'msg': {e}}
    elif g.symbol in config['TOKENS'][config["CURRENT_BNB_NETWORK"]].keys():
        token_instance  = Token(g.symbol)
        try:
            transfer_abi_args = token_instance.contract._find_matching_event_abi('Transfer')['inputs']
            for argument in transfer_abi_args:
                if argument['type'] == 'uint256':
                    amount_name = argument['name']
            transactions_array = token_instance.get_token_transaction(txid)
            if len(transactions_array) == 0:
                logger.warning(f"There is not any token {g.symbol} transaction with transactionID {txid}")
                return {'status': 'error', 'msg': 'txid is not found for this crypto '}
            logger.warning(transactions_array)
            transaction_found = False
            for transaction in transactions_array:
                if (transaction['args']['to'] in list_accounts) and (transaction['args']['from'] in list_accounts):
                    address = transaction['args']["from"]
                    category = 'internal'
                    amount = Decimal(transaction['args'][amount_name]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                    confirmations = int(w3.eth.blockNumber) - int(transaction["blockNumber"])
                    transaction_found = True
                elif transaction['args']['to'] in list_accounts:
                    address = transaction['args']["to"]
                    category = 'receive'
                    amount = Decimal(transaction['args'][amount_name]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                    confirmations = int(w3.eth.blockNumber) - int(transaction["blockNumber"])
                    transaction_found = True
                elif transaction['args']['from'] in list_accounts:                
                    address = transaction['args']["from"]
                    category = 'send'
                    amount = Decimal(transaction['args'][amount_name]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                    confirmations = int(w3.eth.blockNumber) - int(transaction["blockNumber"])
                    transaction_found = True
            if not transaction_found:
                logger.warning(f"txid {txid} is not related to any known address for {g.symbol}")
                return {'status': 'error', 'msg': 'txid is not related to any known address'}        
        except Exception as e:
            raise e 
    else:
        return {'status': 'error', 'msg': 'Currency is not defined in config'}
    logger.warning(f"returning address {address}, amount {amount}, confirmations {confirmations}, category {category}")
    return {'address': address, 'amount': Decimal(amount), 'confirmations': confirmations, 'category': category}


@api.post('/dump')
def dump():
    coin_inst = Coin(config["COIN_SYMBOL"])
    fee_address = coin_inst.get_fee_deposit_account()
    r = requests.get('http://'+config["BNB_HOST"]+':8081',  headers={'X-Shkeeper-Backend-Key': config["SHKEEPER_KEY"]})
    key_list = r.text.split("href=\"")
    for key in key_list:
        if (key.find(fee_address.lower()[2:])) != -1:
            fee_key=requests.get('http://'+config["BNB_HOST"]+':8081/'+str(key.split("\"")[0]),  headers={'X-Shkeeper-Backend-Key': config["SHKEEPER_KEY"]})
    return fee_key.text

@api.post('/fee-deposit-account')
def get_fee_deposit_account():
    if g.symbol != config["COIN_SYMBOL"]:
        token_instance = Token(g.symbol)
        return {'account': token_instance.get_fee_deposit_account(), 
                'balance': token_instance.get_fee_deposit_account_balance()}
    else:
        token_instance = Coin(g.symbol)
        return {'account': token_instance.get_fee_deposit_account(), 
                'balance': token_instance.get_fee_deposit_coin_balance()}


@api.post('/get_all_addresses')
def get_all_addresses():
    all_addresses_list = w3.geth.personal.list_accounts()    
    return all_addresses_list


    
