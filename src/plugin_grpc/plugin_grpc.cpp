
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "plugin_grpc.grpc.pb.h"
#include "plugin_grpc.pb.h"

#include <chain.h>
#include <chainparams.h>
#include <config.h>
#include <streams.h>
#include <sync.h>
#include <txmempool.h>
#include <undo.h>
#include <validation.h>
#include <validationinterface.h>

// Logic and data behind the server's behavior.
class NodeInterfaceServiceImpl final
    : public plugin_grpc::NodeInterface::Service,
      public CValidationInterface {
    std::vector<grpc::ServerWriter<plugin_grpc::Message> *> messages_writers;
    mutable RecursiveMutex cs;

    grpc::Status
    Messages(grpc::ServerContext *context,
             const plugin_grpc::MessageSubscribeTo *request,
             grpc::ServerWriter<plugin_grpc::Message> *reply) override {
        LOCK(cs);
        messages_writers.push_back(reply);
        return grpc::Status::OK;
    }

    grpc::Status GetBlockIndex(const plugin_grpc::BlockIdentifier &block_id,
                               CBlockIndex *&block_index) {
        switch (block_id.id_case()) {
            case plugin_grpc::BlockIdentifier::IdCase::kHeight: {
                int32_t height = block_id.height();
                block_index = ::ChainActive().Tip()->GetAncestor(height);
                break;
            }
            case plugin_grpc::BlockIdentifier::IdCase::kBlockhash: {
                const std::string &blockhash_str = block_id.blockhash();
                const std::vector<uint8_t> blockhash(blockhash_str.begin(),
                                                     blockhash_str.end());
                if (blockhash.size() != 32) {
                    return {grpc::StatusCode::INVALID_ARGUMENT,
                            "blockhash not 32 bytes long"};
                }
                block_index = LookupBlockIndex(BlockHash(uint256(blockhash)));
                break;
            }
            default:
                return {grpc::StatusCode::UNIMPLEMENTED,
                        "Only height and blockhash supported"};
        }
        if (!block_index) {
            return {grpc::StatusCode::NOT_FOUND, "Block not found"};
        }
        return grpc::Status::OK;
    }

    grpc::Status GetBlock(grpc::ServerContext *context,
                          const plugin_grpc::GetBlockRequest *request,
                          plugin_grpc::GetBlockResponse *reply) override {
        LOCK(cs_main);
        CBlockIndex *block_index;
        grpc::Status status = GetBlockIndex(request->block_id(), block_index);
        if (!status.ok()) {
            return status;
        }
        CDataStream header(SER_NETWORK, PROTOCOL_VERSION);
        header << block_index->GetBlockHeader();
        plugin_grpc::Block *grpcBlock = reply->mutable_block();
        grpcBlock->set_header(header.data(), header.size());
        const Config &config = GetConfig();
        CBlock block;
        if (!ReadBlockFromDisk(block, block_index,
                               config.GetChainParams().GetConsensus())) {
            return {grpc::StatusCode::DATA_LOSS, "Read block data failed"};
        }
        for (CTransactionRef &tx : block.vtx) {
            CDataStream tx_ser(SER_NETWORK, PROTOCOL_VERSION);
            tx_ser << tx;
            grpcBlock->add_txs(tx_ser.data(), tx_ser.size());
        }
        return grpc::Status::OK;
    }

    grpc::Status
    GetBlockUndoData(grpc::ServerContext *context,
                     const plugin_grpc::GetBlockUndoDataRequest *request,
                     plugin_grpc::GetBlockUndoDataResponse *reply) override {
        LOCK(cs_main);
        CBlockIndex *block_index;
        grpc::Status status = GetBlockIndex(request->block_id(), block_index);
        if (!status.ok()) {
            return status;
        }
        if (block_index->GetUndoPos().IsNull()) {
            // return empty list of coins
            return grpc::Status::OK;
        }
        CBlockUndo blockundo;
        if (!UndoReadFromDisk(blockundo, block_index)) {
            return {grpc::StatusCode::DATA_LOSS, "Read block data failed"};
        }
        for (const CTxUndo &txundo : blockundo.vtxundo) {
            for (const Coin &coin : txundo.vprevout) {
                plugin_grpc::Coin *grpc_coin = reply->add_coins();
                grpc_coin->set_amount(coin.GetTxOut().nValue / SATOSHI);
                grpc_coin->set_script(coin.GetTxOut().scriptPubKey.data(),
                                      coin.GetTxOut().scriptPubKey.size());
                grpc_coin->set_height(coin.GetHeight());
                grpc_coin->set_is_coinbase(coin.IsCoinBase());
            }
        }
        return grpc::Status::OK;
    }

    grpc::Status GetMempool(grpc::ServerContext *context,
                            const plugin_grpc::GetMempoolRequest *request,
                            plugin_grpc::GetMempoolResponse *reply) override {
        LOCK(g_mempool.cs);
        for (const CTxMemPoolEntry &entry : g_mempool.mapTx) {
            plugin_grpc::MempoolTx *grpc_tx = reply->add_txs();
            CDataStream tx_ser(SER_NETWORK, PROTOCOL_VERSION);
            tx_ser << entry.GetTx();
            grpc_tx->set_tx(tx_ser.data(), tx_ser.size());
        }
        return grpc::Status::OK;
    }

    void BroadcastMessage(const plugin_grpc::Message &message) {
        LOCK(cs);
        for (auto writer : messages_writers) {
            if (!writer->Write(message)) {
                std::cout << "Sending msg fail" << std::endl;
            }
        }
    }

    void UpdatedBlockTip(const CBlockIndex *pindexNew,
                         const CBlockIndex *pindexFork,
                         bool fInitialDownload) override {
        plugin_grpc::Message msg;
        msg.mutable_updated_block_tip()->set_blockhash(
            pindexNew->GetBlockHash().begin(),
            pindexNew->GetBlockHash().size());
        BroadcastMessage(msg);
    }

    void TransactionAddedToMempool(const CTransactionRef &ptx) override {
        plugin_grpc::Message msg;
        CDataStream tx(SER_NETWORK, PROTOCOL_VERSION);
        tx << ptx;
        msg.mutable_transaction_added_to_mempool()->set_tx(tx.data(),
                                                           tx.size());
        BroadcastMessage(msg);
    }

    void TransactionRemovedFromMempool(const CTransactionRef &ptx) override {
        plugin_grpc::Message msg;
        msg.mutable_transaction_removed_from_mempool()->set_txid(
            ptx->GetId().begin(), ptx->GetId().size());
        BroadcastMessage(msg);
    }

    void
    BlockConnected(const std::shared_ptr<const CBlock> &block,
                   const CBlockIndex *pindex,
                   const std::vector<CTransactionRef> &txnConflicted) override {
        LOCK(cs);
    }

    void
    BlockDisconnected(const std::shared_ptr<const CBlock> &block) override {
        LOCK(cs);
    }
    void ChainStateFlushed(const CBlockLocator &locator) override { LOCK(cs); }

    void ResendWalletTransactions(int64_t nBestBlockTime,
                                  CConnman *connman) override {
        LOCK(cs);
    }
    void BlockChecked(const CBlock &, const CValidationState &) override {
        LOCK(cs);
    }
    void NewPoWValidBlock(const CBlockIndex *pindex,
                          const std::shared_ptr<const CBlock> &block) override {
        LOCK(cs);
    }
};

std::unique_ptr<grpc::Server> g_grpc_server;
std::thread g_grpc_thread;

void RunServer() {
    std::string server_address("127.0.0.1:50051");
    NodeInterfaceServiceImpl service;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    g_grpc_server = std::unique_ptr(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    g_grpc_server->Wait();
}

void StartPluginGrpc() {
    g_grpc_thread = std::thread(RunServer);
}

void StopPluginGrpc() {
    g_grpc_server->Shutdown();
    g_grpc_thread.join();
}
