import requests
import prometheus_client
from prometheus_client import generate_latest, Info, Gauge
from web3 import Web3, HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware 

from . import metrics_blueprint
from ..config import config
from ..models import Settings, db


prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)


def get_latest_release(name):
    if name == 'geth':
        url = 'https://api.github.com/repos/bnb-chain/bsc/releases/latest'
    else:
        return False
    data = requests.get(url).json()
    version = data["tag_name"].split('v')[1]
    info = { key:data[key] for key in ["name", "tag_name", "published_at"] }
    info['version'] = version
    return info

def get_all_metrics():
    w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if w3.is_connected:
        response = {}
        last_fullnode_block_number = w3.eth.block_number
        response['last_fullnode_block_number'] = last_fullnode_block_number
        response['last_fullnode_block_timestamp'] = w3.eth.get_block(w3.toHex(last_fullnode_block_number))['timestamp']
    
        geth_version = w3.client_version
        geth_version = geth_version.split('v')[1].split('-')[0]
        response['geth_version'] = geth_version
    
        pd = Settings.query.filter_by(name = 'last_block').first()
        last_checked_block_number = int(pd.value)
        response['bnb_wallet_last_block'] = last_checked_block_number
        block =  w3.eth.get_block(w3.to_hex(last_checked_block_number))
        response['bnb_wallet_last_block_timestamp'] = block['timestamp']
        response['bnb_fullnode_status'] = 1
        return response
    else:
        response['bnb_fullnode_status'] = 0
        return response

geth_last_release = Info(
    'geth_last_release',
    'Version of the latest release from https://github.com/bnb-chain/bsc/releases'
)


geth_last_release.info(get_latest_release('geth'))

geth_fullnode_version = Info('geth_fullnode_version', 'Current geth version in use')

bnb_fullnode_status = Gauge('bnb_fullnode_status', 'Connection status to bnb fullnode')

bnb_fullnode_last_block = Gauge('bnb_fullnode_last_block', 'Last block loaded to the fullnode', )
bnb_wallet_last_block = Gauge('bnb_wallet_last_block', 'Last checked block ')  #.set_function(lambda: BlockScanner().get_last_seen_block_num())

bnb_fullnode_last_block_timestamp = Gauge('bnb_fullnode_last_block_timestamp', 'Last block timestamp loaded to the fullnode', )
bnb_wallet_last_block_timestamp = Gauge('bnb_wallet_last_block_timestamp', 'Last checked block timestamp')

@metrics_blueprint.get("/metrics")
def get_metrics():
    response = get_all_metrics()
    if response['bnb_fullnode_status'] == 1:
        geth_fullnode_version.info({'version': response['geth_version']})
        bnb_fullnode_last_block.set(response['last_fullnode_block_number'])
        bnb_fullnode_last_block_timestamp.set(response['last_fullnode_block_timestamp'])
        bnb_wallet_last_block.set(response['bnb_wallet_last_block'])
        bnb_wallet_last_block_timestamp.set(response['bnb_wallet_last_block_timestamp'])
        bnb_fullnode_status.set(response['bnb_fullnode_status'])
    else:
        bnb_fullnode_status.set(response['bnb_fullnode_status'])


    return generate_latest().decode()