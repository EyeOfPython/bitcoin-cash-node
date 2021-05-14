#!/usr/bin/env python3
# Copyright (c) 2021 The Bitcoin Cash developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""Test the Plugin gRPC interface."""
import struct
from io import BytesIO
import hashlib

from test_framework.blocktools import create_raw_transaction
from test_framework.test_framework import BitcoinTestFramework
from test_framework.messages import CTransaction, CTxIn, COutPoint, CTxOut
from test_framework.script import CScript, OP_HASH160, OP_EQUAL, hash160
from test_framework.txtools import pad_tx
from test_framework.util import (
    assert_equal,
    assert_raises_rpc_error,
)


def hash256(data):
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


class PluginGRPCTest(BitcoinTestFramework):
    def set_test_params(self):
        self.num_nodes = 1
        self.setup_clean_chain = True

    def skip_test_if_missing_module(self):
        self.skip_if_no_py3_grpc()

    def run_test(self):
        try:
            self._grpc_test()
        finally:
            self.log.debug("Destroying GRPC context")

    def _grpc_test(self):
        import grpc, plugin_grpc_pb2_grpc

        node = self.nodes[0]
        channel = grpc.insecure_channel('localhost:50051')
        stub = plugin_grpc_pb2_grpc.NodeInterfaceStub(channel)
        self._tests_genesis(node, stub)
        self._tests_get_block_errors(stub)
        self._tests_get_block_undo_data_errors(stub)
        self._tests_send_tx(node, stub)
        self._tests_update_chain_tip(node, stub)

    def _tests_genesis(self, node, stub):
        import plugin_grpc_pb2
        # Assert genesis block matches RPC's
        rpc_genesis_blockhash = node.getblockhash(0)
        block_id = plugin_grpc_pb2.BlockIdentifier(height=0)
        response = stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
            block_id=block_id,
        ))
        blockhash = hash256(response.block.header)
        assert_equal(blockhash[::-1].hex(), rpc_genesis_blockhash)
        assert_equal(len(response.block.metadata), 0)
        assert_equal(len(response.block.txs), 1)
        # Empty block undo data
        undo_response = stub.GetBlockUndoData(plugin_grpc_pb2.GetBlockUndoDataRequest(
            block_id=block_id,
        ))
        assert_equal(len(undo_response.coins), 0)

        # Same header when using blockhash
        block_id = plugin_grpc_pb2.BlockIdentifier(blockhash=blockhash)
        response2 = stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
            block_id=block_id,
        ))
        assert_equal(response2.block.header, response.block.header)
        assert_equal(len(response2.block.metadata), 0)
        assert_equal(len(response2.block.txs), 1)
        # Empty block undo data
        undo_response = stub.GetBlockUndoData(plugin_grpc_pb2.GetBlockUndoDataRequest(
            block_id=block_id,
        ))
        assert_equal(len(undo_response.coins), 0)

    def _tests_get_block_errors(self, stub):
        import grpc, plugin_grpc_pb2
        # Clean chain -> block 1 doesn't exist
        try:
            stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
                block_id=plugin_grpc_pb2.BlockIdentifier(height=1),
            ))
        except grpc.RpcError as e:
            assert_equal(e.code(), grpc.StatusCode.NOT_FOUND)
            assert_equal(e.details(), "Block not found")

        # blockhash doesn't exist
        try:
            stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
                block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=bytes(32)),
            ))
        except grpc.RpcError as e:
            assert_equal(e.code(), grpc.StatusCode.NOT_FOUND)
            assert_equal(e.details(), "Block not found")

        # blockhash not 32 bytes
        try:
            stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
                block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=bytes(31)),
            ))
        except grpc.RpcError as e:
            assert_equal(e.code(), grpc.StatusCode.INVALID_ARGUMENT)
            assert_equal(e.details(), "blockhash not 32 bytes long")

    def _tests_get_block_undo_data_errors(self, stub):
        import grpc, plugin_grpc_pb2
        # Clean chain -> block 1 doesn't exist
        try:
            stub.GetBlockUndoData(plugin_grpc_pb2.GetBlockUndoDataRequest(
                block_id=plugin_grpc_pb2.BlockIdentifier(height=1),
            ))
        except grpc.RpcError as e:
            assert_equal(e.code(), grpc.StatusCode.NOT_FOUND)
            assert_equal(e.details(), "Block not found")

        # blockhash doesn't exist
        try:
            stub.GetBlockUndoData(plugin_grpc_pb2.GetBlockUndoDataRequest(
                block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=bytes(32)),
            ))
        except grpc.RpcError as e:
            assert_equal(e.code(), grpc.StatusCode.NOT_FOUND)
            assert_equal(e.details(), "Block not found")

        # blockhash not 32 bytes
        try:
            stub.GetBlockUndoData(plugin_grpc_pb2.GetBlockUndoDataRequest(
                block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=bytes(31)),
            ))
        except grpc.RpcError as e:
            assert_equal(e.code(), grpc.StatusCode.INVALID_ARGUMENT)
            assert_equal(e.details(), "blockhash not 32 bytes long")

    def _tests_send_tx(self, node, stub):
        import grpc, plugin_grpc_pb2
        # OP_TRUE P2SH address
        address = node.decodescript('51')['p2sh']

        # Generate block and query it
        hashes = node.generatetoaddress(1, address)
        blockhash = bytes.fromhex(hashes[0])[::-1]
        response = stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
            block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=blockhash),
        ))
        assert_equal(hash256(response.block.header)[::-1].hex(), blockhash[::-1].hex())
        assert_equal(len(response.block.metadata), 0)
        assert_equal(len(response.block.txs), 1)
        coinbase_tx = CTransaction()
        coinbase_tx.deserialize(BytesIO(response.block.txs[0]))
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
        response = stub.GetMempool(plugin_grpc_pb2.GetMempoolRequest())
        assert_equal(len(response.txs), 0)

        # Broadcast tx
        node.sendrawtransaction(tx.serialize().hex())
        # Mempool now has tx
        response = stub.GetMempool(plugin_grpc_pb2.GetMempoolRequest())
        assert_equal(len(response.txs), 1)
        assert_equal(response.txs[0].tx.hex(), tx.serialize().hex())

        # Mine tx
        hashes = node.generatetoaddress(1, address)
        # Mempool empty again
        response = stub.GetMempool(plugin_grpc_pb2.GetMempoolRequest())
        assert_equal(len(response.txs), 0)

        # Block contains tx
        blockhash = bytes.fromhex(hashes[0])[::-1]
        response = stub.GetBlock(plugin_grpc_pb2.GetBlockRequest(
            block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=blockhash),
        ))
        assert_equal(hash256(response.block.header)[::-1].hex(), blockhash[::-1].hex())
        assert_equal(len(response.block.txs), 2)
        assert_equal(response.block.txs[1].hex(), tx.serialize().hex())

        # Undo data has coin data
        response = stub.GetBlockUndoData(plugin_grpc_pb2.GetBlockUndoDataRequest(
            block_id=plugin_grpc_pb2.BlockIdentifier(blockhash=blockhash),
        ))
        assert_equal(len(response.coins), 1)
        assert_equal(response.coins[0].amount, coinbase_value)
        assert_equal(response.coins[0].script.hex(), CScript([OP_HASH160, hash160(b'\x51'), OP_EQUAL]).hex())
        assert_equal(response.coins[0].height, 1)
        assert_equal(response.coins[0].is_coinbase, True)

    def _tests_update_chain_tip(self, node, stub):
        import grpc, plugin_grpc_pb2
        # OP_TRUE P2SH address
        address = node.decodescript('51')['p2sh']
        messages = stub.Messages(plugin_grpc_pb2.MessageSubscribeTo())
        hashes = node.generatetoaddress(1, address)
        print(next(iter(messages)))
        assert False


if __name__ == '__main__':
    PluginGRPCTest().main()
