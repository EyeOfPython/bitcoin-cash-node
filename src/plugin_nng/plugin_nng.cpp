#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>

#include <nng/nng.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/supplemental/util/platform.h>

#include <thread>

#include <chain.h>
#include <chainparams.h>
#include <config.h>
#include <streams.h>
#include <sync.h>
#include <txmempool.h>
#include <undo.h>
#include <validation.h>
#include <validationinterface.h>

#include "plugin_interface_generated.h"

#ifndef PARALLEL
#define PARALLEL 128
#endif

struct work {
    enum { INIT, RECV, WAIT, SEND } state;
    nng_aio *aio;
    nng_msg *msg;
    nng_ctx ctx;
};

void fatal(const char *func, int rv) {
    fprintf(stderr, "%s: %s\n", func, nng_strerror(rv));
    exit(1);
}

bool GetBlockIndex(const PluginInterface::GetBlockRequest *request,
                   CBlockIndex *&block_index) {
    switch (request->block_id_type()) {
        case PluginInterface::BlockIdentifier_Height: {
            int32_t height = request->block_id_as_Height()->height();
            block_index = ::ChainActive().Tip()->GetAncestor(height);
            break;
        }
        case PluginInterface::BlockIdentifier_Blockhash: {
            const flatbuffers::Vector<uint8_t> *blockhash_fb = request->block_id_as_Blockhash()->blockhash();
            const std::vector<uint8_t> blockhash(blockhash_fb->begin(), blockhash_fb->end());
            
            if (blockhash.size() != 32) {
                return false; // "blockhash not 32 bytes long"
            }
            block_index = LookupBlockIndex(BlockHash(uint256(blockhash)));
            break;
        }
        default:
            return false; // "Only height and blockhash supported"
    }
    if (!block_index) {
        return false; // "Block not found"
    }
    return true;
}

void server_cb(void *arg) {
    work *work = (struct work *)arg;
    nng_msg *msg;
    int rv;

    switch (work->state) {
        case work::INIT:
            work->state = work::RECV;
            nng_ctx_recv(work->ctx, work->aio);
            break;
        case work::RECV: {
            if ((rv = nng_aio_result(work->aio)) != 0) {
                fatal("nng_ctx_recv", rv);
            }
            msg = nng_aio_get_msg(work->aio);
            flatbuffers::Verifier verifier((uint8_t *)nng_msg_body(msg), nng_msg_len(msg));
            if (!verifier.VerifyBuffer<PluginInterface::Rpc>()) {
                std::cout << "Invalid message received" << std::endl;
                return;
            }
            const PluginInterface::Rpc *rpc = flatbuffers::GetRoot<PluginInterface::Rpc>(nng_msg_body(msg));
            switch (rpc->rpc_type()) {
                case PluginInterface::RpcType_GetBlockRequest: {
                    LOCK(cs_main);
                    const PluginInterface::GetBlockRequest *request = rpc->rpc_as_GetBlockRequest();
                    CBlockIndex *block_index;
                    if (!GetBlockIndex(request, block_index)) {
                        std::cout << "GetBlockIndex failed" << std::endl;
                        return;
                    }
                    CDataStream header(SER_NETWORK, PROTOCOL_VERSION);
                    header << block_index->GetBlockHeader();
                    flatbuffers::FlatBufferBuilder builder(1<<14);
                    const Config &config = GetConfig();
                    CBlock block;
                    if (!ReadBlockFromDisk(block, block_index,
                                           config.GetChainParams().GetConsensus())) {
                        std::cout << "Read block data failed" << std::endl;
                    }
                    std::vector<flatbuffers::Offset<PluginInterface::Tx>> txs_fb;
                    for (const CTransactionRef &tx : block.vtx) {
                        CDataStream tx_ser(SER_NETWORK, PROTOCOL_VERSION);
                        tx_ser << tx;
                        txs_fb.push_back(PluginInterface::CreateTx(builder, builder.CreateVector((uint8_t*)tx_ser.data(), tx_ser.size())));
                    }
                    auto block_fb = PluginInterface::CreateBlock(
                        builder,
                        builder.CreateVector((uint8_t*)header.data(), header.size()),
                        builder.CreateVector<flatbuffers::Offset<PluginInterface::BlockMetadata>>({}),
                        builder.CreateVector(txs_fb));
                    auto response = PluginInterface::CreateGetBlockResponse(builder, block_fb);
                    builder.Finish(response);
                    flatbuffers::FlatBufferBuilder result_builder(builder.GetSize() + 1024);
                    auto result = PluginInterface::CreateRpcResult(
                        result_builder,
                        true,
                        result_builder.CreateString(""),
                        result_builder.CreateVector(builder.GetBufferPointer(), builder.GetSize()));
                    result_builder.Finish(result);
                    nng_msg *msg_response;
                    if ((rv = nng_msg_alloc(&msg_response, result_builder.GetSize())) != 0) {
                        fatal("nng_msg_alloc", rv);
                    }
                    memcpy(nng_msg_body(msg_response), (void *)result_builder.GetBufferPointer(), result_builder.GetSize());
                    nng_aio_set_msg(work->aio, msg_response);
                    work->msg = NULL;
                    work->state = work::SEND;
                    nng_ctx_send(work->ctx, work->aio);
                    return;
                }
                default:
                    std::cout << "Got unimplemented message type" << std::endl;
                    return;
            }
            // We could add more data to the message here.
            //flatbuffers::FlatBufferBuilder builder(1024);
            //PluginInterface::CreateBlock(builder, builder.CreateVector(std::vector<uint8_t>({1, 2, 3})));

            /*if ((rv = nng_msg_alloc(&msg, 6)) != 0) {
                fatal("nng_msg_alloc", rv);
            }
            const char *response = "Hello!";
            memcpy(nng_msg_body(msg), (void *)response, 6);
            nng_aio_set_msg(work->aio, msg);
            work->msg = NULL;
            work->state = work::SEND;
            nng_ctx_send(work->ctx, work->aio);*/
            break;
        }
        case work::SEND:
            if ((rv = nng_aio_result(work->aio)) != 0) {
                nng_msg_free(work->msg);
                fatal("nng_ctx_send", rv);
            }
            work->state = work::RECV;
            nng_ctx_recv(work->ctx, work->aio);
            break;
        default:
            fatal("bad state!", NNG_ESTATE);
            break;
    }
}

void init_work(nng_socket sock, work &w) {
    int rv;

    if ((rv = nng_aio_alloc(&w.aio, server_cb, &w)) != 0) {
        fatal("nng_aio_alloc", rv);
    }
    if ((rv = nng_ctx_open(&w.ctx, sock)) != 0) {
        fatal("nng_ctx_open", rv);
    }
    w.state = work::INIT;
}

void server() {
    const char *url = "tcp://127.0.0.1:5000";
    nng_socket sock;
    work works[PARALLEL];
    int rv;
    int i;

    printf("Listening with NNG at %s\n", url);

    /*  Create the socket. */
    rv = nng_rep0_open(&sock);
    if (rv != 0) {
        fatal("nng_rep0_open", rv);
    }

    for (i = 0; i < PARALLEL; i++) {
        init_work(sock, works[i]);
    }

    if ((rv = nng_listen(sock, url, NULL, 0)) != 0) {
        fatal("nng_listen", rv);
    }

    for (i = 0; i < PARALLEL; i++) {
        server_cb(&works[i]); // this starts them going (INIT state)
    }

    for (;;) {
        nng_msleep(3600000); // neither pause() nor sleep() portable
    }
}

/*
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
}*/

std::thread g_grpc_thread;

void StartPluginNng() {
    g_grpc_thread = std::thread(server);
}

void StopPluginNng() {
    // g_grpc_server->Shutdown();
    // g_grpc_thread.join();
}
