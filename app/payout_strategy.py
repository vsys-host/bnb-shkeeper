# from web3 import Web3, HTTPProvider
# from web3.middleware import geth_poa_middleware
# from decimal import Decimal
# from pprint import pprint
# import copy


# from .config import config
# from .token import Token, Coin
# from .logging import logger
# from .unlock_acc import get_account_password

# w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
# w3.middleware_onion.inject(geth_poa_middleware, layer=0)



# def get_payout_steps(payout_symbol, payout_list):
#     steps = []
#     fee = Decimal(config['MAX_PRIORITY_FEE'])
#     account_index = 0
#     token_instance = Token(payout_symbol)
#     account_dict = {}
#     # multiplier = config['MULTIPLIER']

#     for account in token_instance.get_accounts_with_tokens():
#         account_dict.update({account:{}})
#     logger.warning(account_dict)
#     for account in account_dict:
#         buf_dict = {}
#         buf_dict.update({'balance': token_instance.get_account_balance(account)})
#         buf_dict.update({'gas': 0})
#         buf_dict.update({'need_gas': 0})
#         buf_dict.update({'need_crypto': Decimal(0)})
#         buf_dict.update({'num_of_tran': 0})
#         buf_dict.update({'virt_bal': token_instance.get_account_balance(account)})
#         account_dict[account].update(buf_dict)
#     logger.warning(account_dict)
#     for payout in payout_list:              
#         list_of_account = list(account_dict.keys())  
#         current_account = list_of_account[account_index]    
#         if Decimal(payout["amount"]) < Decimal(account_dict[current_account]['virt_bal']):
#             #gas  = token_instance.contract.functions.transfer(payout['dest'], int((Decimal(payout['amount']) * 10** (token_instance.contract.functions.decimals().call())))).estimateGas({'from': current_account})
#             #gas  = token_instance.contract.functions.transfer(payout['dest'], int(Decimal(payout['amount']) )).estimateGas({'from': current_account})
#             logger.warning(payout['dest'], int((Decimal(payout['amount']) * 10** (token_instance.contract.functions.decimals().call()))),{'from': current_account})
#             gas  = token_instance.contract.functions.transfer(payout['dest'], int((Decimal(payout['amount']) * 10** (token_instance.contract.functions.decimals().call())))).estimateGas({'from': current_account})
#             #logger.warning((payout['dest'], int((Decimal(payout['amount']) * 10** (token_instance.contract.functions.decimals().call())))).estimateGas({'from': current_account}))

#             steps.append({'from': current_account, 'to': payout['dest'], 'amount': payout["amount"], 'gas': gas})
#             account_dict[current_account]['virt_bal'] = Decimal(account_dict[current_account]['virt_bal']) - Decimal(payout["amount"])
#             account_dict[current_account]['num_of_tran'] = account_dict[current_account]['num_of_tran'] + 1
#             account_dict[current_account]['need_gas'] = account_dict[current_account]['need_gas'] + gas
#             if (Decimal(account_dict[current_account]['virt_bal']) == Decimal(0)):
#                 account_index = account_index + 1                
#         else:       
#             need_payout = Decimal(payout["amount"])
#             while need_payout > 0 :
#                 print(steps)
#                 current_account = list_of_account[account_index] 
#                 if need_payout >= Decimal(account_dict[current_account]['virt_bal']):
#                     can_send = Decimal(account_dict[current_account]['virt_bal'])
#                 else:
#                      can_send = need_payout
#                 logger.warning(payout['dest'], int((Decimal(can_send) * 10** (token_instance.contract.functions.decimals().call()))),current_account)
#                 gas  = token_instance.contract.functions.transfer(payout['dest'], int((Decimal(can_send) * 10** (token_instance.contract.functions.decimals().call())))).estimateGas({'from': current_account})
#                 # gas  = token_instance.contract.functions.transfer(payout['dest'], int(Decimal(can_send) )).estimateGas({'from': current_account})
#                 steps.append({'from': current_account, 'to': payout['dest'], 'amount': can_send, 'gas': gas})
#                 account_dict[current_account]['virt_bal'] = Decimal(account_dict[current_account]['virt_bal']) - Decimal(can_send)
#                 account_dict[current_account]['num_of_tran'] = account_dict[current_account]['num_of_tran'] + 1
#                 account_dict[current_account]['need_gas'] = account_dict[current_account]['need_gas'] + gas
#                 need_payout = Decimal(need_payout) - can_send
#                 if (Decimal(account_dict[current_account]['virt_bal']) == Decimal(0)):
#                     account_index = account_index + 1  
#     gas_price = token_instance.get_gas_price()
#     max_fee_per_gas = ( token_instance.provider.fromWei(gas_price, "ether") + Decimal(fee) ) * Decimal(config['MULTIPLIER'])

#     #add to need_crypto gas which need for sending crypto to tokken acc
#     # eth_transaction = {"from": w3.toChecksumAddress(w3.geth.personal.list_accounts()[0]),
#     #                         "to": w3.toChecksumAddress(payout['dest']), 
#     #                         "value": w3.toWei(0, "ether")}  # transaction example for counting gas
#     #eth_gas_count = w3.eth.estimate_gas(eth_transaction) *  Decimal(config['MULTIPLIER'])
#     for account in account_dict:
#         if account_dict[account]['num_of_tran'] > 0:
#             #account_dict[account]['need_gas'] = account_dict[current_account]['need_gas'] + eth_gas_count 
#             account_dict[account]['need_crypto'] = account_dict[current_account]['need_gas'] * max_fee_per_gas * Decimal(config['MULTIPLIER'])
            

#     logger.warning(steps, account_dict)
#     logger.warning('-------------------->', account_dict)
#     return steps, account_dict
#     # pprint(steps)


# def seed_fees(payout_symbol, account_dict, fee):
#     transaction_list = []  
#     token_instance = Token(payout_symbol) 
#     gas_price = token_instance.get_gas_price()
#     max_fee_per_gas = int(( gas_price + token_instance.provider.toWei(Decimal(fee), "ether") ) * Decimal(config['MULTIPLIER']))
#     transaction = {"from": token_instance.provider.toChecksumAddress(token_instance.get_fee_deposit_account()),
#                     "to": token_instance.provider.toChecksumAddress(token_instance.get_accounts_with_tokens()[0]), 
#                     "value": token_instance.provider.toWei(0, "ether")}  # transaction example for counting gas
#     gas_count = int(token_instance.provider.eth.estimate_gas(transaction) *  Decimal(config['MULTIPLIER'])) #make it bigger for sure

#     for account in account_dict: 
#         if Decimal(account_dict[account]['num_of_tran']) > 0:
    
#             transaction_list.append(token_instance.provider.geth.personal.send_transaction({"from": token_instance.provider.toChecksumAddress(token_instance.get_fee_deposit_account()), 
#                                                                 "to": token_instance.provider.toChecksumAddress(account),
#                                                                 "value": token_instance.provider.toHex(token_instance.provider.toWei(account_dict[account]['need_crypto'], "ether")),
#                                                                 "gas": token_instance.provider.toHex(gas_count),
#                                                                 "maxFeePerGas":  token_instance.provider.toHex(max_fee_per_gas),
#                                                                 "maxPriorityFeePerGas": token_instance.provider.toHex(token_instance.provider.toWei(fee, "ether"))}, get_account_password()))
#     logger.warning(transaction_list)                                     
#     return transaction_list


# def make_payout_steps(payout_symbol, steps):
#     token_instance = Token(payout_symbol)
#     transaction_list = []
#     payout_results = []
#     payout_buf_dict = {}

#     logger.warning("we are in make steps")

#     for step in steps:
#         payout_buf_dict.update({step['to']: {'sent': [], 'txids': []}})


#     for step in steps:
#         logger.warning(f"make {step}")
#         token_instance.provider.geth.personal.unlock_account(token_instance.provider.toChecksumAddress(step['from'].lower()), get_account_password(), int(config['UNLOCK_ACCOUNT_TIME']))
#         #tx_hash = token_contract.functions.transfer('address_to', 100).transact({'from': 'address_from'})
        
#         # keep in mind tokens with decimals 
#         txid = token_instance.contract.functions.transfer(token_instance.provider.toChecksumAddress(step['to']),
#                                                                           int((Decimal(step['amount']) * 10** (token_instance.contract.functions.decimals().call())))).transact({'from': token_instance.provider.toChecksumAddress(step['from'].lower()), 
#                                                                                                                                                                                  'gas':step['gas'], 
#                                                                                                                                                                                  'maxFeePerGas': token_instance.provider.toWei(( token_instance.provider.fromWei(token_instance.get_gas_price(), "ether") + Decimal(config['MAX_PRIORITY_FEE']) ) * Decimal(config['MULTIPLIER']) * Decimal(config['PRICE_MULTIPLIER']) , 'ether'), 
#                                                                                                                                                                                  'maxPriorityFeePerGas':   token_instance.provider.toWei(Decimal(config['MAX_PRIORITY_FEE']), 'ether')}) # without * Decimal(config['MULTIPLIER'])

#         # logger.warning({'from': token_instance.provider.toChecksumAddress(step['from'].lower()), 'gas':step['gas'],  'maxFeePerGas': token_instance.provider.toWei(( token_instance.provider.fromWei(token_instance.get_gas_price(), "ether") + Decimal(config['MAX_PRIORITY_FEE']) ) * Decimal(config['MULTIPLIER']) * Decimal(config['PRICE_MULTIPLIER']) , 'ether'), 'maxPriorityFeePerGas':   token_instance.provider.toWei(config['MAX_PRIORITY_FEE'], 'ether')})

#         token_instance.provider.geth.personal.lock_account(token_instance.provider.toChecksumAddress(step['from'].lower()))
#         payout_buf_dict[step['to']]['txids'].append(txid.hex())
#         payout_buf_dict[step['to']]['sent'].append(Decimal(step['amount']))
      
    
#     for receiver in payout_buf_dict:
#         payout_results.append({
#                 "dest": receiver,
#                 "amount": float(sum(payout_buf_dict[receiver]['sent'])),
#                 "status": "success",
#                 "txids": payout_buf_dict[receiver]['txids'],
#             })

#     logger.warning(payout_results)
#     logger.warning(transaction_list)
#     return payout_results

# def payout_eth(destination, amount, fee,):
#     payout_results = []
#     payout_transactions = []
#     amount = Decimal(amount)
#     fee = Decimal(fee)

#     if not w3.isAddress(destination):
#         raise Exception(f"Address {destination} is not valid ethereum address")           
    
#     multiplier = Decimal(config['MULTIPLIER']) # make max fee per gas as *MULTIPLIER of base price + fee
#     transaction = {"from": w3.toChecksumAddress(w3.geth.personal.list_accounts()[0]),
#                             "to": w3.toChecksumAddress(destination), 
#                             "value": w3.toWei(0, "ether")}  # transaction example for counting gas
#     gas_count = w3.eth.estimate_gas(transaction)
    
#     def get_accounts_for_payout(destination, amount, fee, gas_count):
#         '''
#         Return list of accounts, from which shoud be payout done
#         '''
#         wallet = {}
#         balance_list = []            
#         for account in w3.geth.personal.list_accounts():
#             try:
#                 wallet.update({account : w3.fromWei(w3.eth.get_balance(account), "ether")})
#             except Exception as e:
#                 raise Exception(f"Get error: {e}, when trying get balance")                
        
#         max_fee_per_gas = ( w3.fromWei(w3.eth.gas_price, "ether") + Decimal(fee) ) * multiplier

#         for account, balance in wallet.items():
#             balance_list.append(balance)
#         balance_list.sort(reverse=True)
#         enough_funds = False    
#         balances_used = []
#         funds_to_send = [] 
#         payout_accounts = []   
#         available_amount = Decimal("0")
#         # checking if we can pay using 1 transaction from account with max funds

#         #######
#         can_send = balance_list[0] - ( gas_count * max_fee_per_gas )
#         if can_send >= amount:
#             for account, value in wallet.items():
#                         if balance_list[0] == value:
#                             payout_accounts.append(account) 
#                             funds_to_send.append(amount)    
#             return [payout_accounts, funds_to_send]
#         else:           
#             for balance in balance_list:
#                 can_send = balance - ( gas_count * max_fee_per_gas )
#                 if amount > available_amount + can_send:
#                     available_amount = available_amount + can_send # all in ether
#                     balances_used.append(balance)
#                     funds_to_send.append(can_send)
#                 elif amount < available_amount + can_send:
#                     should_send = amount - available_amount
#                     available_amount = available_amount + should_send # all in ether
#                     balances_used.append(balance)
#                     funds_to_send.append(should_send)
#                     enough_funds = True
#                     break
#                 else:
#                     available_amount = available_amount + can_send # all in ether
#                     balances_used.append(balance)
#                     funds_to_send.append(can_send)
#                     enough_funds = True
#                     break
#             if not enough_funds:
#                 raise Exception(f"Cannot make payout, hasn't enough funds")
#             else:                        
#                 for balance in balances_used:
#                     for account, value in wallet.items():
#                         if balance == value:
#                             payout_accounts.append(account)
#                 return [payout_accounts, funds_to_send]
#     list_of_accounts = get_accounts_for_payout(destination, amount, fee, gas_count)
#     for num, account in enumerate(list_of_accounts[0]):                    
#         max_fee_per_gas = int(( w3.eth.gas_price + w3.toWei(Decimal(fee), 'ether') ) * multiplier)
#         sending_amount = list_of_accounts[1][num]

#         trans = w3.geth.personal.send_transaction({"from": w3.toChecksumAddress(account), 
#                                                                            "to": w3.toChecksumAddress(destination),
#                                                                            "value": w3.toHex(w3.toWei(sending_amount, "ether")),
#                                                                            "gas": w3.toHex(gas_count),
#                                                                            "maxFeePerGas":  w3.toHex(max_fee_per_gas),
#                                                                            "maxPriorityFeePerGas": w3.toHex(w3.toWei(fee, "ether"))}, get_account_password())
#         # trans = trans.hex()
#         payout_transactions.append(trans.hex())
#     payout_results.append({
#             "dest": destination,
#             "amount": float(sending_amount),
#             "status": "success",
#             "txids": payout_transactions,
#         })

#     return payout_results



# def multipayout_eth(payout_list, fee):
#     payout_results = []
#     #payout_transactions = []
#     payout_list = payout_list
#     #amount = Decimal(amount)
#     fee = Decimal(fee)

#     for payout in payout_list:
#         if not w3.isAddress(payout['dest']):
#             raise Exception(f"Address {payout['dest']} is not valid ethereum address")           
    
#     multiplier = Decimal(config['MULTIPLIER']) # make max fee per gas as *MULTIPLIER of base price + fee
#     transaction = {"from": w3.toChecksumAddress(w3.geth.personal.list_accounts()[0]),
#                             "to": w3.toChecksumAddress(payout['dest']), 
#                             "value": w3.toWei(0, "ether")}  # transaction example for counting gas
#     gas_count = w3.eth.estimate_gas(transaction)
    
#     def get_accounts_for_payout(payout_list, fee, gas_count):
#         '''
#         Return list of accounts, from which shoud be payout done
#         '''
#         wallet = {}
#         balance_list = []  
#         result = {}       

#         coin_instance = Coin(config["COIN_SYMBOL"])   

#         wallet = coin_instance.get_all_balances() 
        
#         logger.warning(wallet)
#         max_fee_per_gas = ( w3.fromWei(w3.eth.gas_price, "ether") + Decimal(fee) ) * multiplier

#         for account, balance in wallet.items():
#             balance_list.append(balance)
#         balance_list.sort(reverse=True)
#         enough_funds = False    
#         balances_used = []   
#         available_amount = Decimal("0")
        
#         # checking if we can pay using 1 transaction from account with max funds

#         #######
#         for payout in payout_list:
#             funds_to_send = [] 
#             payout_accounts = []
#             can_send = balance_list[0] - ( gas_count * max_fee_per_gas )
#             amount = payout['amount']
#             if can_send >= amount:
#                 for account, value in wallet.items():
#                             if balance_list[0] == value:
#                                 payout_accounts.append(account) 
#                                 funds_to_send.append(amount)
#                                 break
                               
#                 result.update({payout['dest']:[copy.deepcopy(payout_accounts), copy.deepcopy(funds_to_send)]})    
#                 #return result #[payout_accounts, funds_to_send]
#             else:           
#                 for balance in balance_list:
#                     can_send = balance - ( gas_count * max_fee_per_gas )
#                     if amount > available_amount + can_send:
#                         available_amount = available_amount + can_send # all in ether
#                         balances_used.append(balance)
#                         funds_to_send.append(can_send)
#                     elif amount < available_amount + can_send:
#                         should_send = amount - available_amount
#                         available_amount = available_amount + should_send # all in ether
#                         balances_used.append(balance)
#                         funds_to_send.append(should_send)
#                         enough_funds = True
#                         break
#                     else:
#                         available_amount = available_amount + can_send # all in ether
#                         balances_used.append(balance)
#                         funds_to_send.append(can_send)
#                         enough_funds = True
#                         break
#                 if not enough_funds:
#                     raise Exception(f"Cannot make payout, hasn't enough funds")
#                 else:                        
#                     for balance in balances_used:
#                         for account, value in wallet.items():
#                             if balance == value:
#                                 payout_accounts.append(account)
#                     result.update({payout['dest']:[copy.deepcopy(payout_accounts), copy.deepcopy(funds_to_send)]})
#         logger.warning(result)
#         return result
#                     #return [payout_accounts, funds_to_send]
#     list_of_accounts = get_accounts_for_payout(payout_list, fee, gas_count)
#     payout_buf_dict = {}
#     for receiver in list_of_accounts:
#         payout_buf_dict.update({receiver: {'sent': [], 'txids': []}})

#     for receiver in list_of_accounts:
#         for num, account in enumerate(list_of_accounts[receiver][0]):                    
#             max_fee_per_gas = int(( w3.eth.gas_price + w3.toWei(Decimal(fee), 'ether') ) * multiplier)
#             sending_amount = list_of_accounts[receiver][1][num]
    
#             trans = w3.geth.personal.send_transaction({"from": w3.toChecksumAddress(account), 
#                                                                                "to": w3.toChecksumAddress(receiver),
#                                                                                "value": w3.toHex(w3.toWei(sending_amount, "ether")),
#                                                                                "gas": w3.toHex(gas_count),
#                                                                                "maxFeePerGas":  w3.toHex(max_fee_per_gas),
#                                                                                "maxPriorityFeePerGas": w3.toHex(w3.toWei(fee, "ether"))}, get_account_password())
#             # trans = trans.hex()
#             payout_buf_dict[receiver]['txids'].append(trans.hex())
#             payout_buf_dict[receiver]['sent'].append(Decimal(sending_amount))
#             # payout_transactions.append(trans.hex())
#         payout_results.append({
#                 "dest": receiver,
#                 "amount": float(sum(payout_buf_dict[receiver]['sent'])),
#                 "status": "success",
#                 "txids": payout_buf_dict[receiver]['txids'],
#             })
    
#     return payout_results


# def drain_account(account, destination):
#     drain_results = []
#     fee = Decimal(fee)
#     account_balance = Decimal(0)

#     if not w3.isAddress(destination):
#         raise Exception(f"Address {destination} is not valid ethereum address") 

#     if not w3.isAddress(account):
#         raise Exception(f"Address {account} is not valid ethereum address")          
    
#     multiplier = Decimal(config['MULTIPLIER']) # make max fee per gas as *MULTIPLIER of base price + fee
#     transaction = {"from": w3.toChecksumAddress(account),
#                             "to": w3.toChecksumAddress(destination), 
#                             "value": w3.toWei(0, "ether")}  # transaction example for counting gas
#     gas_count = w3.eth.estimate_gas(transaction)
#     max_fee_per_gas = ( w3.fromWei(w3.eth.gas_price, "ether") + Decimal(fee) ) * multiplier
#     try:
#         account_balance = w3.fromWei(w3.eth.get_balance(account), "ether")
#     except Exception as e:
#         raise Exception(f"Get error: {e}, when trying get balance")  
                     
#     can_send = account_balance - ( gas_count * max_fee_per_gas )
#     trans = w3.geth.personal.send_transaction({"from": w3.toChecksumAddress(account), 
#                                                                            "to": w3.toChecksumAddress(destination),
#                                                                            "value": w3.toHex(w3.toWei(can_send, "ether")),
#                                                                            "gas": w3.toHex(gas_count),
#                                                                            "maxFeePerGas":  w3.toHex(max_fee_per_gas),
#                                                                            "maxPriorityFeePerGas": w3.toHex(w3.toWei(fee, "ether"))}, get_account_password())

#     drain_results.append({
#             "dest": destination,
#             "amount": float(can_send),
#             "status": "success",
#             "txids": {[trans.hex()]},
#         })

#     return drain_results

