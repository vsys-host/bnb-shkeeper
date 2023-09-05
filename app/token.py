from web3 import HTTPProvider, Web3
from web3.middleware import geth_poa_middleware 
from decimal import Decimal
from flask import current_app as app
import time


from .logging import logger
from .config import config, get_contract_abi, get_contract_address, get_min_token_transfer_threshold
from .models import Accounts, Settings, db
from .unlock_acc import get_account_password

class Coin:
    w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    def __init__(self, symbol, init=True):
        self.symbol = symbol        
        self.fullnode = config["FULLNODE_URL"]
        self.provider = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
        self.provider.middleware_onion.inject(geth_poa_middleware, layer=0)

    def get_transaction_price(self):
        gas_price = self.provider.eth.gasPrice
        fee = Decimal(config['MAX_PRIORITY_FEE'])
        multiplier = Decimal(config['MULTIPLIER']) # make max fee per gas as *MULTIPLIER of base price + fee
        # add to need_crypto gas which need for sending crypto to tokken acc
        max_fee_per_gas = ( self.provider.fromWei(gas_price, "ether") + Decimal(fee) ) 
        eth_transaction = {"from": self.provider.toChecksumAddress(self.get_fee_deposit_account()),
                                "to": self.provider.toChecksumAddress(self.get_fee_deposit_account()), 
                                "value": self.provider.toWei(0, "ether")}  # transaction example for counting gas

        payout_multiplier = Decimal(config['PAYOUT_MULTIPLIER'])
        eth_gas_count = self.provider.eth.estimate_gas(eth_transaction)
        eth_gas_count =  int(eth_gas_count *  payout_multiplier)
        gas_price = self.provider.eth.gasPrice
        max_fee_per_gas = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) * multiplier
        price = eth_gas_count  * max_fee_per_gas
        return price

    def set_fee_deposit_account(self):
        coin_instance = Coin(config["COIN_SYMBOL"])
        new_address = coin_instance.provider.geth.personal.new_account(get_account_password())
        crypto_str = config["COIN_SYMBOL"]
        with app.app_context():
            db.session.add(Accounts(address = new_address, 
                                        crypto = crypto_str,
                                        amount = 0,
                                        type = "fee_deposit"
                                        ))
            db.session.commit()
            db.session.close()
            db.engine.dispose() 

    def get_fee_deposit_account(self):
        try:
            pd = Accounts.query.filter_by(type = "fee_deposit").first()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later")
        if not pd:
            self.set_fee_deposit_account()
        pd = Accounts.query.filter_by(type = "fee_deposit").first()
        return pd.address
    
    def get_fee_deposit_coin_balance(self):
        deposit_account = self.get_fee_deposit_account()
        amount = Decimal(self.provider.fromWei(self.provider.eth.get_balance(deposit_account), "ether"))
        return amount

    def get_all_balances(self):
        balances = {}
        try:
            pd = Accounts.query.filter_by(crypto = self.symbol,).all()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later")
        if not pd:
            raise Exception(f"There is not any account with {self.symbol} crypto in database")
        else:
            for account in pd:
                if account.type != "fee_deposit":
                    balances.update({account.address: Decimal(account.amount)})
            return balances
        
    def make_multipayout_eth(self, payout_list, fee,):
        payout_results = []
        payout_list = payout_list
        fee = Decimal(fee)
    
        for payout in payout_list:
            if not self.provider.isAddress(payout['dest']):
                raise Exception(f"Address {payout['dest']} is not valid ethereum address") 

        for payout in payout_list:
            if not self.provider.isChecksumAddress(payout['dest']):
                logger.warning(f"Provided address {payout['dest']} is not checksum address, converting to checksum address")
                payout['dest'] = self.provider.toChecksumAddress(payout['dest'])
                logger.warning(f"Changed to {payout['dest']} which is checksum address")
         
        multiplier = Decimal(config['MULTIPLIER']) # make max fee per gas as *MULTIPLIER of base price + fee
        max_payout_amount = Decimal(0)
        for payout in payout_list:
            if payout['amount'] > max_payout_amount:
                max_payout_amount = payout['amount']
        transaction = {"from": self.provider.toChecksumAddress(self.get_fee_deposit_account()),
                                "to": self.provider.toChecksumAddress(payout_list[0]['dest']), 
                                "value": self.provider.toWei(max_payout_amount, "ether")}  # transaction example for counting gas
        payout_multiplier = Decimal(config['PAYOUT_MULTIPLIER'])
        gas_count = self.provider.eth.estimate_gas(transaction)
        gas_count = int(gas_count * payout_multiplier)
        gas_price = self.provider.eth.gasPrice
        max_fee_per_gas = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) * multiplier
        # Check if enouth funds for multipayout on account
        should_pay  = Decimal(0)
        for payout in payout_list:
            should_pay = should_pay + Decimal(payout['amount'])
        should_pay = should_pay + len(payout_list) * (max_fee_per_gas * gas_count)
        have_crypto = self.get_fee_deposit_coin_balance()
        if have_crypto < should_pay:
            raise Exception(f"Have not enough crypto on fee account, need {should_pay} have {have_crypto}")
        else:
            for payout in payout_list:
                test_transaction = {"from": self.provider.toChecksumAddress(self.get_fee_deposit_account()),
                                    "to": self.provider.toChecksumAddress(payout['dest']),
                                    "value":  self.provider.toWei(payout['amount'], "ether")}  # transaction example for counting gas

                gas_count = self.provider.eth.estimate_gas(test_transaction)
                gas_count = int(gas_count * payout_multiplier)       
                trans =  self.provider.geth.personal.send_transaction({"from":  self.provider.toChecksumAddress(self.get_fee_deposit_account()), 
                                                                    "to":  self.provider.toChecksumAddress(payout['dest']),
                                                                    "value":  self.provider.toHex(self.provider.toWei(payout['amount'], "ether")),
                                                                    "gas":  self.provider.toHex(gas_count),
                                                                    "maxFeePerGas":   self.provider.toHex(self.provider.toWei(max_fee_per_gas, 'ether')),
                                                                      }, get_account_password())
        
            
                payout_results.append({
                    "dest": payout['dest'],
                    "amount": float(payout['amount']),
                    "status": "success",
                    "txids": [trans.hex()],
                })

        
            return payout_results
   
    def drain_account(self, account, destination):
        drain_results = []
        fee = Decimal(config['MAX_PRIORITY_FEE'])
        account_balance = Decimal(0)
    
        if not self.provider.isAddress(destination):
            raise Exception(f"Address {destination} is not valid ethereum address") 
    
        if not self.provider.isAddress(account):
            raise Exception(f"Address {account} is not valid ethereum address")  

        if not self.provider.isChecksumAddress(destination):
                logger.warning(f"Provided address {destination} is not checksum address, converting to checksum address")
                destination = self.provider.toChecksumAddress(destination)
                logger.warning(f"Changed to {destination} which is checksum address") 
        
        if account == destination:
            logger.warning(f"Fee-deposit account, skip")
            return False     
        
        multiplier = Decimal(config['MULTIPLIER']) # make max fee per gas as *MULTIPLIER of base price + fee
        transaction = {"from":  self.provider.toChecksumAddress(account),
                                "to":  self.provider.toChecksumAddress(destination), 
                                "value":  self.provider.toWei(0, "ether")}  # transaction example for counting gas
        gas_count =  self.provider.eth.estimate_gas(transaction)
        max_fee_per_gas = (  self.provider.fromWei( self.provider.eth.gas_price, "ether" ) + Decimal(fee) ) * multiplier
        try:
            account_balance =  self.provider.fromWei( self.provider.eth.get_balance(account), "ether")
        except Exception as e:
            raise Exception(f"Get error: {e}, when trying get balance")

        if Decimal(config['MIN_TRANSFER_THRESHOLD']) > account_balance :
            logger.warning(f"Balance {account_balance} is lower than MIN_TRANSFER_THRESHOLD {Decimal(config['MIN_TRANSFER_THRESHOLD'])}, skip draining ")             
            #raise Exception(f"Cannot send funds, not enough for paying fee")  
            return False

        can_send = account_balance - ( gas_count * max_fee_per_gas )

        if can_send <= 0:
            logger.warning(f"Cannot send funds, {can_send} not enough for paying fee")             
            #raise Exception(f"Cannot send funds, not enough for paying fee")  
            return False
        else:
            trans =  self.provider.geth.personal.send_transaction({"from":  self.provider.toChecksumAddress(account), 
                                                                    "to":  self.provider.toChecksumAddress(destination),
                                                                    "value":  self.provider.toHex(self.provider.toWei(can_send, "ether")),
                                                                    "gas":  self.provider.toHex(gas_count),
                                                                    "maxFeePerGas":   self.provider.toHex(self.provider.toWei(max_fee_per_gas, 'ether')),
                                                                }, get_account_password())
        
            
            drain_results.append({
                    "dest": destination,
                    "amount": float(can_send),
                    "status": "success",
                    "txids": [trans.hex()],
                })
        
            return drain_results


class Token:
    w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    def __init__(self, symbol, init=True):
        self.symbol = symbol        
        self.contract_address = get_contract_address(symbol)
        self.abi = get_contract_abi(symbol)
        self.fullnode = config["FULLNODE_URL"]
        self.provider = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
        self.provider.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.contract = self.provider.eth.contract(address=self.contract_address, abi=self.abi)

    def get_all_transfers(self, from_block, to_block):
        filter = self.contract.events.Transfer.createFilter(fromBlock=self.provider.toHex(from_block), 
                                                                toBlock=self.provider.toHex(to_block))
        transactions = filter.get_all_entries()
        return transactions

    def get_eth_transaction_price(self):
        gas_price = self.get_gas_price()
        fee = Decimal(config['MAX_PRIORITY_FEE'])
        # add to need_crypto gas which need for sending crypto to tokken acc
        max_fee_per_gas = ( self.provider.fromWei(gas_price, "ether") + Decimal(fee) ) 
        eth_transaction = {"from": self.provider.toChecksumAddress(self.get_fee_deposit_account()),
                                "to": self.provider.toChecksumAddress(self.get_fee_deposit_account()), 
                                "value": self.provider.toWei(0, "ether")}  # transaction example for counting gas
        eth_gas_count = self.provider.eth.estimate_gas(eth_transaction)
        eth_gas_count =  eth_gas_count *  Decimal(config['MULTIPLIER'])
        # for account in account_dict:
        price = eth_gas_count  * max_fee_per_gas * Decimal(config['MULTIPLIER'])
        return price

    def get_account_balance(self, address): 
        try:
            pd = Accounts.query.filter_by(crypto = self.symbol, address = address).first()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later") 
        if not pd:  
            raise Exception(f"There is no account {address} related with {self.symbol} crypto in database") 
        else:
            return pd.amount
        
    def get_account_balance_from_fullnode(self, address):
        balance = Decimal(self.contract.functions.balanceOf(self.provider.toChecksumAddress(address)).call())
        normalized_balance = balance / Decimal(10** (self.contract.functions.decimals().call()))
        return normalized_balance

    def get_token_transaction(self, txid):
        transaction_arr = []
        block_number = self.provider.eth.get_transaction(txid)['blockNumber']
        all_transfers = self.get_all_transfers(block_number, block_number)
        for transaction in all_transfers:
            if transaction['transactionHash'].hex() == txid:
                transaction_arr.append(transaction)
        return transaction_arr
        

    def get_token_balance(self):
        try:
            pd = Accounts.query.filter_by(crypto = self.symbol).all()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later")
        if not pd:
            return Decimal("0")
        else:
            balance = Decimal("0")
            for account in pd:
                balance = balance + account.amount
            return balance

    def get_accounts_with_tokens(self):
        try:
            pd = Accounts.query.filter_by(crypto = self.symbol).all()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later")
        if not pd:
            raise Exception(f"There is no accounts with {self.symbol} crypto") 
        else:
            list_accounts = []
            for account in pd:
                if account.amount > 0:
                    list_accounts.append(account.address)            
            return list_accounts

    def get_coin_transaction_fee(self):
        address = self.get_fee_deposit_account()
        fee = Decimal(config['MAX_PRIORITY_FEE'])
        gas  = self.contract.functions.transfer(address, int((Decimal(0) * 10** (self.contract.functions.decimals().call())))).estimateGas({'from': address})
        gas = int(gas * Decimal(config['MULTIPLIER']))
        gas_price = self.get_gas_price()
        max_fee_per_gas = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) #* Decimal(config['MULTIPLIER'])
        need_crypto = gas * max_fee_per_gas
        return need_crypto

    def get_gas_price(self):
        return self.provider.eth.gasPrice

    def check_eth_address(self, address):
        return self.provider.isAddress(address)

    def set_fee_deposit_account(self):
        coin_instance = Coin(config["COIN_SYMBOL"])
        new_address = coin_instance.provider.geth.personal.new_account(get_account_password())
        crypto_str = config["COIN_SYMBOL"]
        with app.app_context():
            db.session.add(Accounts(address = new_address, 
                                        crypto = crypto_str,
                                        amount = 0,
                                        type = "fee_deposit"
                                        ))
            db.session.commit()
            db.session.close()
            db.engine.dispose() 

    def get_fee_deposit_account(self):
        try:
            pd = Accounts.query.filter_by(type = "fee_deposit").first()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later")
        if not pd:
            self.set_fee_deposit_account()
        pd = Accounts.query.filter_by(type = "fee_deposit").first()
        return pd.address
        
    def get_fee_deposit_account_balance(self):
        address = self.get_fee_deposit_account()
        amount = Decimal(self.provider.fromWei(self.provider.eth.get_balance(address), "ether"))
        return amount
    
    def get_fee_deposit_token_balance(self):
        deposit_account = self.get_fee_deposit_account()
        balance = Decimal(self.contract.functions.balanceOf(self.provider.toChecksumAddress(deposit_account)).call())
        normalized_balance = balance / Decimal(10** (self.contract.functions.decimals().call()))
        return normalized_balance
    
    def make_token_multipayout(self, payout_list, fee,):
        payout_results = []
        payout_list = payout_list
        fee = Decimal(fee)

        if len(payout_list) == 0:
            raise Exception(f"Payout list cannot be empty")
    
        need_tokens = 0 
        for payout in payout_list:
            if not self.provider.isAddress(payout['dest']):
                raise Exception(f"Address {payout['dest']} is not valid ethereum address") 
            need_tokens = need_tokens + payout['amount']

        for payout in payout_list:
            if not self.provider.isChecksumAddress(payout['dest']):
                logger.warning(f"Provided address {payout['dest']} is not checksum address, converting to checksum address")
                payout['dest'] = self.provider.toChecksumAddress(payout['dest'])
                logger.warning(f"Changed to {payout['dest']} which is checksum address")
        
        have_tokens = self.get_fee_deposit_token_balance()
        if need_tokens > have_tokens:
            raise Exception(f"Have not enough tokens on fee account, need {need_tokens} have {have_tokens}")
        
        payout_account = self.get_fee_deposit_account()
        
        gas  = self.contract.functions.transfer(payout_list[0]['dest'], int((Decimal(payout_list[0]['amount']) * 10** (self.contract.functions.decimals().call())))).estimateGas({'from': payout_account})
        gas = int(gas * Decimal(config['MULTIPLIER']))
        gas_price = self.get_gas_price()
        max_fee_per_gas = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) #* Decimal(config['MULTIPLIER'])
        need_crypto = gas * max_fee_per_gas
        need_crypto_for_multipayout = need_crypto * len(payout_list) # approximate сalc just for checking 
        have_crypto = self.get_fee_deposit_account_balance()
        if need_crypto_for_multipayout > have_crypto:
            raise Exception(f"Have not enough crypto on fee account, need {need_crypto_for_multipayout} have {have_crypto}")
        else:
            for payout in payout_list:

                gas  = self.contract.functions.transfer(payout['dest'], int((Decimal(payout['amount']) * 10** (self.contract.functions.decimals().call())))).estimateGas({'from': payout_account})
                gas = int(gas * Decimal(config['MULTIPLIER']))
                gas_price = self.get_gas_price()
                max_fee_per_gas = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) #* Decimal(config['MULTIPLIER'])

                self.provider.geth.personal.unlock_account(self.provider.toChecksumAddress(payout_account.lower()), get_account_password(), int(config['UNLOCK_ACCOUNT_TIME']))      
                txid = self.contract.functions.transfer(self.provider.toChecksumAddress(payout['dest']),
                   int((Decimal(payout['amount']) * 10** (self.contract.functions.decimals().call())))).transact({'from': self.provider.toChecksumAddress(payout_account.lower()), 
                                                                                                          'gas': gas, 
                                                                                                          'maxFeePerGas': self.provider.toWei(max_fee_per_gas, 'ether'), 
                                                                                                          'maxPriorityFeePerGas': self.provider.toWei(max_fee_per_gas, 'ether')}) # without * Decimal(config['MULTIPLIER'])
                self.provider.geth.personal.lock_account(self.provider.toChecksumAddress(payout_account.lower()))
                payout_results.append({
                "dest": payout['dest'],
                "amount": float(payout['amount']),
                "status": "success",
                "txids": [txid.hex()],
            })
                
        return payout_results
     
    def drain_tocken_account(self, account, destination):

        results = []
        
        if not self.check_eth_address(destination):
            raise Exception(f"Address {destination} is not valid ethereum address")     
        if not self.check_eth_address(account):
            raise Exception(f"Address {account} is not valid ethereum address")  
        if not self.provider.isChecksumAddress(destination):
                logger.warning(f"Provided address {destination} is not checksum address, converting to checksum address")
                destination = self.provider.toChecksumAddress(destination)
                logger.warning(f"Changed to {destination} which is checksum address")         
        if account == destination:
            logger.warning(f"Fee-deposit account, skip")
            return False  

        can_send = self.get_account_balance_from_fullnode(account)  

        if Decimal(get_min_token_transfer_threshold(self.symbol)) > can_send :
            logger.warning(f"Balance {can_send} is lower than min_token_transfer_threshold {Decimal(get_min_token_transfer_threshold(self.symbol))}, skip draining ")             
            #raise Exception(f"Cannot send funds, not enough for paying fee")  
            return False

        if can_send <= 0:
            return False
        else:            
            fee = Decimal(config['MAX_PRIORITY_FEE'])
            gas  = self.contract.functions.transfer(destination, int((Decimal(can_send) * 10** (self.contract.functions.decimals().call())))).estimateGas({'from': account})
            gas = int(gas * Decimal(config['MULTIPLIER']))
            gas_price = self.get_gas_price()
            max_fee_per_gas = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) #* Decimal(config['MULTIPLIER'])
            need_crypto = gas * max_fee_per_gas
            # if there is not enough BNB for sending tokens
            logger.warning(f'gas: {str(gas)}\n gas_price: {str(gas_price)}\n need_crypto: {str(need_crypto)}\n balance: {str(Decimal(self.provider.fromWei(self.provider.eth.get_balance(account), "ether"))  )}')
            if Decimal(self.provider.fromWei(self.provider.eth.get_balance(account), "ether")) < need_crypto:            
                need_to_send = need_crypto - self.provider.fromWei(self.provider.eth.get_balance(account), "ether") 
                transaction = {"from": self.provider.toChecksumAddress(self.get_fee_deposit_account()),
                               "to": self.provider.toChecksumAddress(account), 
                               "value": self.provider.toWei(0, "ether")}  # transaction example for counting gas
                gas_coin_count = int(self.provider.eth.estimate_gas(transaction) *  Decimal(config['MULTIPLIER'])) #make it bigger for sure
                max_fee_per_gas_coin = ( Decimal(self.provider.fromWei(gas_price, "ether")) + Decimal(fee) ) * Decimal(config['MULTIPLIER'])
                txid = self.provider.geth.personal.send_transaction({"from": self.provider.toChecksumAddress(self.get_fee_deposit_account()), 
                                                                        "to": self.provider.toChecksumAddress(account),
                                                                        "value": self.provider.toHex(self.provider.toWei(need_to_send, "ether")),
                                                                        "gas": self.provider.toHex(gas_coin_count),
                                                                        "maxFeePerGas":  self.provider.toHex(self.provider.toWei(max_fee_per_gas_coin, 'ether')),
                                                                        }, get_account_password())
                logger.warning(f'send coins to token account: {str(txid.hex())}')
                time.sleep(int(config['SLEEP_AFTER_SEEDING']))
            # Send tokens to the fee account            
            self.provider.geth.personal.unlock_account(self.provider.toChecksumAddress(account.lower()), get_account_password(), int(config['UNLOCK_ACCOUNT_TIME']))    
            txid = self.contract.functions.transfer(self.provider.toChecksumAddress(destination),
                   int((Decimal(can_send) * 10** (self.contract.functions.decimals().call())))).transact({'from': self.provider.toChecksumAddress(account.lower()), 
                                                                                                          'gas': gas, 
                                                                                                          'maxFeePerGas': self.provider.toWei(max_fee_per_gas, 'ether'), 
                                                                                                          'maxPriorityFeePerGas': self.provider.toWei(max_fee_per_gas, 'ether')})  #self.provider.toWei(Decimal(config['MAX_PRIORITY_FEE']), 'ether')}) # without * Decimal(config['MULTIPLIER'])
            self.provider.geth.personal.lock_account(self.provider.toChecksumAddress(account.lower()))
    
            results.append({
                "dest": destination,
                "amount": float(can_send),
                "status": "success",
                "txids": [txid.hex()],
            })
    
            return results










        

