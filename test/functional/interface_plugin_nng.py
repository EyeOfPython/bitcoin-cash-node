#!/usr/bin/env python3
# Copyright (c) 2021 The Bitcoin Cash developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""Test the Plugin gRPC interface."""

import struct
from io import BytesIO
import hashlib
import asyncio

from test_framework.blocktools import create_block, create_coinbase
from test_framework.test_framework import BitcoinTestFramework
from test_framework.messages import CTransaction, CTxIn, COutPoint, CTxOut, COIN, CBlockHeader
from test_framework.script import CScript, OP_HASH160, OP_EQUAL, hash160
from test_framework.txtools import pad_tx
from test_framework.util import (
    assert_equal,
    assert_raises,
)


def hash256(data):
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()

RPC_URL = "tcp://127.0.0.1:52783"
PUB_URL = "tcp://127.0.0.1:52784"

def get_fb_bytes(obj, name):
    get_item = getattr(obj, name)
    get_length = getattr(obj, f'{name}Length')
    return bytes(get_item(i) for i in range(get_length()))


class PluginGRPCTest(BitcoinTestFramework):
    NUM_GENERATED_COINS = 10

    def set_test_params(self):
        self.num_nodes = 1
        self.setup_clean_chain = True
        self.extra_args = [[
            f"-pluginrpcurl={RPC_URL}",
            f"-pluginpuburl={PUB_URL}",
            # Force the mempool expiry task to run once per second
            "-mempoolexpirytaskperiod=0",
            # Always expire after 1h
            "-mempoolexpiry=1",
        ]]
        self.coin_blocks = []

    def skip_test_if_missing_module(self):
        self.skip_if_no_py3_pynng()
        self.skip_if_no_py3_flatbuffers()
        self.skip_if_no_bitcoind_plugin_interface()

    def run_test(self):
        try:
            asyncio.get_event_loop().run_until_complete(self._nng_test())
        finally:
            self.log.debug("Destroying NNG context")

    async def _nng_test(self):
        import pynng, flatbuffers

        node = self.nodes[0]
        with pynng.Req0() as rpc_sock:
            rpc_sock.dial(RPC_URL)
            await self._tests_genesis(node, rpc_sock)
            await self._tests_get_block_errors(rpc_sock)
            await self._tests_get_block_undo_data_errors(rpc_sock)
            await self._tests_send_tx(node, rpc_sock)
        with pynng.Sub0() as pub_sock:
            pub_sock.dial(PUB_URL)
            await self._tests_update_chain_tip(node, pub_sock)
            await self._tests_transaction_added_to_mempool(node, pub_sock)
            await self._tests_transaction_removed_from_mempool(node, pub_sock)
            await self._test_block_connected(node, pub_sock)
            await self._test_block_disconnected(node, pub_sock)
            await self._test_chain_state_flushed(node, pub_sock)
            await self._test_block_checked(node, pub_sock)
            await self._test_new_pow_valid_block(node, pub_sock)

    def _make_get_block_request_fbb(self, *, height=None, blockhash=None):
        from PluginInterface import (
            RpcCall,
            RpcCallType,
            GetBlockRequest,
            BlockIdentifier,
            BlockHeight,
            BlockHash,
        )
        import flatbuffers
        fbb = flatbuffers.Builder()
        if height is not None:
            BlockHeight.BlockHeightStart(fbb)
            BlockHeight.BlockHeightAddHeight(fbb, height)
            block_height = BlockHeight.BlockHeightEnd(fbb)
            GetBlockRequest.GetBlockRequestStart(fbb)
            GetBlockRequest.GetBlockRequestAddBlockIdType(fbb, BlockIdentifier.BlockIdentifier.Height)
            GetBlockRequest.GetBlockRequestAddBlockId(fbb, block_height)
        else:
            block_hash_fb = fbb.CreateByteVector(blockhash)
            BlockHash.BlockHashStart(fbb)
            BlockHash.BlockHashAddBlockhash(fbb, block_hash_fb)
            block_hash = BlockHash.BlockHashEnd(fbb)
            GetBlockRequest.GetBlockRequestStart(fbb)
            GetBlockRequest.GetBlockRequestAddBlockIdType(fbb, BlockIdentifier.BlockIdentifier.Blockhash)
            GetBlockRequest.GetBlockRequestAddBlockId(fbb, block_hash)
        get_block_request = GetBlockRequest.GetBlockRequestEnd(fbb)
        RpcCall.RpcCallStart(fbb)
        RpcCall.RpcCallAddRpcType(fbb, RpcCallType.RpcCallType.GetBlockRequest)
        RpcCall.RpcCallAddRpc(fbb, get_block_request)
        rpc = RpcCall.RpcCallEnd(fbb)
        fbb.Finish(rpc)
        return bytes(fbb.Output())

    def _make_get_block_undo_data_request_fbb(self, *, height=None, blockhash=None):
        from PluginInterface import (
            RpcCall,
            RpcCallType,
            GetBlockUndoDataRequest,
            BlockIdentifier,
            BlockHeight,
            BlockHash,
        )
        import flatbuffers
        fbb = flatbuffers.Builder()
        if height is not None:
            BlockHeight.BlockHeightStart(fbb)
            BlockHeight.BlockHeightAddHeight(fbb, height)
            block_height = BlockHeight.BlockHeightEnd(fbb)
            GetBlockUndoDataRequest.GetBlockUndoDataRequestStart(fbb)
            GetBlockUndoDataRequest.GetBlockUndoDataRequestAddBlockIdType(fbb, BlockIdentifier.BlockIdentifier.Height)
            GetBlockUndoDataRequest.GetBlockUndoDataRequestAddBlockId(fbb, block_height)
        else:
            block_hash_fb = fbb.CreateByteVector(blockhash)
            BlockHash.BlockHashStart(fbb)
            BlockHash.BlockHashAddBlockhash(fbb, block_hash_fb)
            block_hash = BlockHash.BlockHashEnd(fbb)
            GetBlockUndoDataRequest.GetBlockUndoDataRequestStart(fbb)
            GetBlockUndoDataRequest.GetBlockUndoDataRequestAddBlockIdType(fbb, BlockIdentifier.BlockIdentifier.Blockhash)
            GetBlockUndoDataRequest.GetBlockUndoDataRequestAddBlockId(fbb, block_hash)
        get_block_request = GetBlockUndoDataRequest.GetBlockUndoDataRequestEnd(fbb)
        RpcCall.RpcCallStart(fbb)
        RpcCall.RpcCallAddRpcType(fbb, RpcCallType.RpcCallType.GetBlockUndoDataRequest)
        RpcCall.RpcCallAddRpc(fbb, get_block_request)
        rpc = RpcCall.RpcCallEnd(fbb)
        fbb.Finish(rpc)
        return bytes(fbb.Output())

    def _make_get_mempool_request_fbs(self):
        from PluginInterface import (
            RpcCall,
            RpcCallType,
            GetMempoolRequest,
        )
        import flatbuffers
        fbb = flatbuffers.Builder()
        GetMempoolRequest.GetMempoolRequestStart(fbb)
        get_mempool_request = GetMempoolRequest.GetMempoolRequestEnd(fbb)
        RpcCall.RpcCallStart(fbb)
        RpcCall.RpcCallAddRpcType(fbb, RpcCallType.RpcCallType.GetMempoolRequest)
        RpcCall.RpcCallAddRpc(fbb, get_mempool_request)
        rpc = RpcCall.RpcCallEnd(fbb)
        fbb.Finish(rpc)
        return bytes(fbb.Output())

    async def _send_request(self, rpc_sock, request, *, timeout=1):
        await asyncio.wait_for(rpc_sock.asend(request), timeout=timeout)

    async def _recv_response(self, rpc_sock, *, expect_error=None, timeout=1):
        from PluginInterface import RpcResult
        response_msg = await asyncio.wait_for(rpc_sock.arecv_msg(), timeout=timeout)
        result = RpcResult.RpcResult.GetRootAsRpcResult(response_msg.bytes, 0)
        if expect_error is not None:
            assert not result.IsSuccess()
            assert_equal(result.ErrorMsg().decode(), expect_error)
        else:
            assert result.IsSuccess()
            return get_fb_bytes(result, 'Data')

    async def _tests_genesis(self, node, rpc_sock):
        from PluginInterface import (
            GetBlockResponse,
            GetBlockUndoDataResponse,
        )
        # Assert genesis block matches RPC's
        rpc_genesis_blockhash = node.getblockhash(0)
        rpc_genesis_block = node.getblock(rpc_genesis_blockhash, 2)

        # Test for using both height and blockhash as reference
        for params in [{'height': 0}, {'blockhash': bytes.fromhex(rpc_genesis_blockhash)[::-1]}]:
            # Test GetBlock
            await self._send_request(rpc_sock, self._make_get_block_request_fbb(**params))
            response = await self._recv_response(rpc_sock)
            response = GetBlockResponse.GetBlockResponse.GetRootAsGetBlockResponse(response, 0)
            block = response.Block()
            blockhash = hash256(get_fb_bytes(block, 'Header'))
            assert_equal(blockhash[::-1].hex(), rpc_genesis_blockhash)
            assert_equal(block.MetadataLength(), 0)
            assert_equal(block.TxsLength(), 1)
            assert_equal(get_fb_bytes(block.Txs(0), 'Raw').hex(), rpc_genesis_block['tx'][0]['hex'])
    
            # Test GetBlockUndoData
            await self._send_request(rpc_sock, self._make_get_block_undo_data_request_fbb(**params))
            response = await self._recv_response(rpc_sock)
            response = GetBlockUndoDataResponse.GetBlockUndoDataResponse.GetRootAsGetBlockUndoDataResponse(response, 0)
            assert_equal(response.CoinsLength(), 0)

    async def _tests_get_block_errors(self, rpc_sock):
        # Clean chain -> block 1 doesn't exist
        await self._send_request(rpc_sock, self._make_get_block_request_fbb(height=1))
        await self._recv_response(rpc_sock, expect_error='Block not found')
        # blockhash doesn't exist
        await self._send_request(rpc_sock, self._make_get_block_request_fbb(blockhash=bytes(32)))
        await self._recv_response(rpc_sock, expect_error='Block not found')
        # blockhash not 32 bytes
        await self._send_request(rpc_sock, self._make_get_block_request_fbb(blockhash=bytes(31)))
        await self._recv_response(rpc_sock, expect_error='Invalid blockhash size, must be 32 bytes')

    async def _tests_get_block_undo_data_errors(self, rpc_sock):
        # Clean chain -> block 1 doesn't exist
        await self._send_request(rpc_sock, self._make_get_block_undo_data_request_fbb(height=1))
        await self._recv_response(rpc_sock, expect_error='Block not found')
        # blockhash doesn't exist
        await self._send_request(rpc_sock, self._make_get_block_undo_data_request_fbb(blockhash=bytes(32)))
        await self._recv_response(rpc_sock, expect_error='Block not found')
        # blockhash not 32 bytes
        await self._send_request(rpc_sock, self._make_get_block_undo_data_request_fbb(blockhash=bytes(31)))
        await self._recv_response(rpc_sock, expect_error='Invalid blockhash size, must be 32 bytes')

    async def _tests_send_tx(self, node, rpc_sock):
        from PluginInterface import (
            GetBlockResponse,
            GetBlockUndoDataResponse,
            GetMempoolResponse,
        )
        # OP_TRUE P2SH address
        address = node.decodescript('51')['p2sh']

        # Generate block and query it
        hashes = node.generatetoaddress(self.NUM_GENERATED_COINS, address)
        self.coin_blocks = hashes[1:]
        blockhash = bytes.fromhex(hashes[0])[::-1]

        await self._send_request(rpc_sock, self._make_get_block_request_fbb(blockhash=blockhash))
        response = await self._recv_response(rpc_sock)
        response = GetBlockResponse.GetBlockResponse.GetRootAsGetBlockResponse(response, 0)
        assert_equal(hash256(get_fb_bytes(response.Block(), 'Header'))[::-1].hex(), blockhash[::-1].hex())
        assert_equal(response.Block().MetadataLength(), 0)
        assert_equal(response.Block().TxsLength(), 1)
        coinbase_tx = CTransaction()
        coinbase_tx.deserialize(BytesIO(get_fb_bytes(response.Block().Txs(0), 'Raw')))
        coinbase_tx.calc_sha256()

        # Mature coinbase tx
        node.generatetoaddress(100, address)

        # Create valid tx
        coinbase_value = coinbase_tx.vout[0].nValue
        p2sh_script = CScript([OP_HASH160, bytes(20), OP_EQUAL])
        tx = CTransaction()
        tx.vin.append(
            CTxIn(COutPoint(coinbase_tx.sha256, 0), CScript([b'\x51'])))
        tx.vout.append(CTxOut(coinbase_value - 1000, p2sh_script))
        pad_tx(tx)

        # Query mempool -> is empty
        await self._send_request(rpc_sock, self._make_get_mempool_request_fbs())
        response = await self._recv_response(rpc_sock)
        response = GetMempoolResponse.GetMempoolResponse.GetRootAsGetMempoolResponse(response, 0)
        assert_equal(response.TxsLength(), 0)

        # Broadcast tx
        node.sendrawtransaction(tx.serialize().hex())
        # Mempool now has tx
        await self._send_request(rpc_sock, self._make_get_mempool_request_fbs())
        response = await self._recv_response(rpc_sock)
        response = GetMempoolResponse.GetMempoolResponse.GetRootAsGetMempoolResponse(response, 0)
        assert_equal(response.TxsLength(), 1)
        assert_equal(get_fb_bytes(response.Txs(0), 'Tx').hex(), tx.serialize().hex())

        # Mine tx
        hashes = node.generatetoaddress(1, address)
        # Mempool empty again
        await self._send_request(rpc_sock, self._make_get_mempool_request_fbs())
        response = await self._recv_response(rpc_sock)
        response = GetMempoolResponse.GetMempoolResponse.GetRootAsGetMempoolResponse(response, 0)
        assert_equal(response.TxsLength(), 0)

        # Block contains tx
        blockhash = bytes.fromhex(hashes[0])[::-1]
        await self._send_request(rpc_sock, self._make_get_block_request_fbb(blockhash=blockhash))
        response = await self._recv_response(rpc_sock)
        response = GetBlockResponse.GetBlockResponse.GetRootAsGetBlockResponse(response, 0)
        assert_equal(hash256(get_fb_bytes(response.Block(), 'Header'))[::-1].hex(), blockhash[::-1].hex())
        assert_equal(response.Block().MetadataLength(), 0)
        assert_equal(response.Block().TxsLength(), 2)
        assert_equal(get_fb_bytes(response.Block().Txs(1), 'Raw').hex(), tx.serialize().hex())

        # Undo data has coin data
        await self._send_request(rpc_sock, self._make_get_block_undo_data_request_fbb(blockhash=blockhash))
        response = await self._recv_response(rpc_sock)
        response = GetBlockUndoDataResponse.GetBlockUndoDataResponse.GetRootAsGetBlockUndoDataResponse(response, 0)
        assert_equal(response.CoinsLength(), 1)
        assert_equal(response.Coins(0).Amount(), coinbase_value)
        assert_equal(get_fb_bytes(response.Coins(0), 'Script').hex(), CScript([OP_HASH160, hash160(b'\x51'), OP_EQUAL]).hex())
        assert_equal(response.Coins(0).Height(), 1)
        assert_equal(response.Coins(0).IsCoinbase(), True)

    async def _recv_message(self, pub_sock, expected_msg_type, timeout=2):
        received_msg = await asyncio.wait_for(pub_sock.arecv_msg(), timeout=timeout)
        actual_msg_type = received_msg.bytes[:12]
        assert_equal(actual_msg_type.decode(), expected_msg_type)
        return received_msg.bytes[12:]

    async def _check_timeout(self, fut, timeout=0.1):
        try:
            await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            pass
        else:
            raise AssertionError("Future didn't timeout")

    def _get_utxo(self, node):
        blockhash = self.coin_blocks.pop(0)
        coinbase_tx = node.getblock(blockhash, 2)['tx'][0]
        vout = 0
        value = int(coinbase_tx['vout'][vout]['value'] * COIN)
        return COutPoint(int(coinbase_tx['txid'], 16), vout), value

    def _create_block(self, node):
        bestblockhash = node.getbestblockhash()
        bestblock = node.getblock(bestblockhash)
        return create_block(
            bestblock['hash'], create_coinbase(bestblock['height'] + 1), bestblock['time'] + 1)

    async def _tests_update_chain_tip(self, node, pub_sock):
        from PluginInterface.UpdatedBlockTip import UpdatedBlockTip
        # OP_TRUE P2SH address
        pub_sock.subscribe('updateblktip')
        address = node.decodescript('51')['p2sh']
        hashes = node.generatetoaddress(1, address)
        msg = await self._recv_message(pub_sock, 'updateblktip')
        msg = UpdatedBlockTip.GetRootAsUpdatedBlockTip(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Blockhash')[::-1].hex(), hashes[0])
        pub_sock.unsubscribe('updateblktip')

    async def _tests_transaction_added_to_mempool(self, node, pub_sock):
        from PluginInterface.TransactionAddedToMempool import TransactionAddedToMempool
        pub_sock.subscribe('mempooltxadd')
        tx = CTransaction()
        outpoint, value = self._get_utxo(node)
        tx.vin.append(
            CTxIn(outpoint, CScript([b'\x51'])))
        tx.vout.append(CTxOut(value - 1000, CScript([OP_HASH160, bytes(20), OP_EQUAL])))
        pad_tx(tx)
        node.sendrawtransaction(tx.serialize().hex())
        msg = await self._recv_message(pub_sock, 'mempooltxadd')
        msg = TransactionAddedToMempool.GetRootAsTransactionAddedToMempool(msg, 0)
        assert_equal(get_fb_bytes(msg.Tx(), 'Raw').hex(), tx.serialize().hex())
        pub_sock.unsubscribe('mempooltxadd')

    async def _tests_transaction_removed_from_mempool(self, node, pub_sock):
        from PluginInterface.TransactionRemovedFromMempool import TransactionRemovedFromMempool
        pub_sock.subscribe('mempooltxrem')
        node.generate(1)  # empty out mempool from previous test
        await self._check_timeout(pub_sock.arecv_msg(), timeout=0.1) # should not send an eviction message
        assert_equal(node.getrawmempool(), []) # mempool should be empty
        tx = CTransaction()
        outpoint, value = self._get_utxo(node)
        tx.vin.append(
            CTxIn(outpoint, CScript([b'\x51'])))
        tx.vout.append(CTxOut(value - 1000, CScript([OP_HASH160, bytes(20), OP_EQUAL])))
        pad_tx(tx)
        txhash = node.sendrawtransaction(tx.serialize().hex())
        entry_time = node.getmempoolentry(txhash)['time']
        node.setmocktime(entry_time + 3605)
        msg = await self._recv_message(pub_sock, 'mempooltxrem', timeout=5)
        msg = TransactionRemovedFromMempool.GetRootAsTransactionRemovedFromMempool(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Txid')[::-1].hex(), tx.hash)
        assert_equal(node.getrawmempool(), [])
        pub_sock.unsubscribe('mempooltxrem')
        node.setmocktime(0)

    async def _test_block_connected(self, node, pub_sock):
        from PluginInterface.BlockConnected import BlockConnected
        pub_sock.subscribe('blkconnected')
        tx = CTransaction()
        outpoint, value = self._get_utxo(node)
        tx.vin.append(
            CTxIn(outpoint, CScript([b'\x51'])))
        tx.vout.append(CTxOut(value - 1000, CScript([OP_HASH160, bytes(20), OP_EQUAL])))
        pad_tx(tx)
        conflicted_txhash = node.sendrawtransaction(tx.serialize().hex())
        tx.vout[0].nValue -= 1 # tweak transaction
        tx.rehash()
        assert conflicted_txhash != tx.hash
        block = self._create_block(node)
        block.vtx.append(tx)
        block.hashMerkleRoot = block.calc_merkle_root()
        block.solve()
        assert_equal(node.submitblock(block.serialize().hex()), None)
        msg = await self._recv_message(pub_sock, 'blkconnected')
        msg = BlockConnected.GetRootAsBlockConnected(msg, 0)
        assert_equal(get_fb_bytes(msg.Block(), 'Header').hex(), CBlockHeader(block).serialize().hex())
        assert_equal(msg.Block().MetadataLength(), 0)
        assert_equal(msg.Block().TxsLength(), 2)
        assert_equal(get_fb_bytes(msg.Block().Txs(1), 'Raw').hex(), tx.serialize().hex())
        assert_equal(msg.TxsConflictedLength(), 1)
        assert_equal(bytes(msg.TxsConflicted(0).Id())[::-1].hex(), conflicted_txhash)
        pub_sock.unsubscribe('blkconnected')

    async def _test_block_disconnected(self, node, pub_sock):
        from PluginInterface.BlockDisconnected import BlockDisconnected
        pub_sock.subscribe('blkdisconctd')
        tip = node.getbestblockhash()
        tipblock = node.getblock(tip)
        reorged_blockhash = node.generate(1)[0]
        block1 = create_block(tip, create_coinbase(tipblock['height'] + 1), tipblock['time'] + 1)
        block1.solve()
        assert_equal(node.submitblock(block1.serialize().hex()), 'inconclusive')
        block2 = create_block(block1.hash, create_coinbase(tipblock['height'] + 2), tipblock['time'] + 2)
        block2.solve()
        assert_equal(node.submitblock(block2.serialize().hex()), None)
        msg = await self._recv_message(pub_sock, 'blkdisconctd')
        msg = BlockDisconnected.GetRootAsBlockDisconnected(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Blockhash')[::-1].hex(), reorged_blockhash)
        pub_sock.unsubscribe('blkdisconctd')

    async def _test_chain_state_flushed(self, node, pub_sock):
        from PluginInterface.ChainStateFlushed import ChainStateFlushed
        pub_sock.subscribe('chainstflush')
        tip = node.getbestblockhash()
        node.gettxoutsetinfo() # forces chain flush
        msg = await self._recv_message(pub_sock, 'chainstflush')
        msg = ChainStateFlushed.GetRootAsChainStateFlushed(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Blockhash')[::-1].hex(), tip)
        pub_sock.unsubscribe('chainstflush')

    async def _test_block_checked(self, node, pub_sock):
        from PluginInterface.BlockChecked import BlockChecked
        from PluginInterface.BlockValidationModeState import BlockValidationModeState
        pub_sock.subscribe('blockchecked')
        # Send valid block
        generated_blockhash = node.generate(1)[0]
        msg = await self._recv_message(pub_sock, 'blockchecked')
        msg = BlockChecked.GetRootAsBlockChecked(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Blockhash')[::-1].hex(), generated_blockhash)
        assert_equal(msg.ValidationState().State(), BlockValidationModeState.Valid)
        assert_equal(msg.ValidationState().RejectReason(), b'')
        assert_equal(msg.ValidationState().DebugMsg(), b'')
        # Send invalid block
        block = self._create_block(node)
        block.vtx.append(CTransaction())  # invalidate merkle root
        block.solve()
        assert_equal(node.submitblock(block.serialize().hex()), 'bad-txnmrklroot')
        msg = await self._recv_message(pub_sock, 'blockchecked')
        msg = BlockChecked.GetRootAsBlockChecked(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Blockhash')[::-1].hex(), block.hash)
        assert_equal(msg.ValidationState().State(), BlockValidationModeState.Invalid)
        assert_equal(msg.ValidationState().RejectReason(), b'bad-txnmrklroot')
        assert_equal(msg.ValidationState().DebugMsg(), b'hashMerkleRoot mismatch')
        pub_sock.unsubscribe('blockchecked')

    async def _test_new_pow_valid_block(self, node, pub_sock):
        from PluginInterface.NewPoWValidBlock import NewPoWValidBlock
        pub_sock.subscribe('newpowvldblk')
        block = self._create_block(node)
        block.solve()
        assert_equal(node.submitblock(block.serialize().hex()), None)
        msg = await self._recv_message(pub_sock, 'newpowvldblk')
        msg = NewPoWValidBlock.GetRootAsNewPoWValidBlock(msg, 0)
        assert_equal(get_fb_bytes(msg, 'Blockheader').hex(), CBlockHeader(block).serialize().hex())
        pub_sock.unsubscribe('newpowvldblk')


if __name__ == '__main__':
    PluginGRPCTest().main()
