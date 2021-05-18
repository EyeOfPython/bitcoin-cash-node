#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/supplemental/util/platform.h>

#include <thread>

#include <chain.h>
#include <chainparams.h>
#include <config.h>
#include <consensus/validation.h>
#include <streams.h>
#include <sync.h>
#include <txmempool.h>
#include <undo.h>
#include <util/system.h>
#include <validation.h>
#include <validationinterface.h>
#include <logging.h>

#include "plugin_interface_generated.h"

enum class PluginRpcWorkerState {
    UNINIT,
    RECV,
    SEND,
};

class PluginRpcServer;

#define NNG_TRY(call)                                                          \
    do {                                                                       \
        int rv = (call);                                                       \
        if (rv != 0) {                                                         \
            fprintf(stderr, "NNG Error: %s\n  at %s:%d: %s\n",                 \
                    nng_strerror(rv), __FILE__, __LINE__, #call);              \
            exit(1);                                                           \
        }                                                                      \
    } while (false)

enum RpcErrorCode {
    NO_ERROR = 0,
    NNG_ERROR,
    INVALID_FLATBUFFER_ENCODING,
    UNKNOWN_RPC_METHOD,
    BLOCK_ID_UNKNOWN_TYPE,
    BLOCKHASH_INVALID_SIZE,
    BLOCK_NOT_FOUND,
    BLOCK_DATA_CORRUPTED,
};

struct RpcResult {
    RpcErrorCode error_code;
    std::vector<uint8_t> data = std::vector<uint8_t>();
};

std::string ErrorMsg(RpcErrorCode code) {
    switch (code) {
        case RpcErrorCode::NO_ERROR:
            return "No error";
        case RpcErrorCode::INVALID_FLATBUFFER_ENCODING:
            return "Invalid flatbuffer encoding";
        case RpcErrorCode::UNKNOWN_RPC_METHOD:
            return "Unknown RPC method";
        case RpcErrorCode::BLOCK_ID_UNKNOWN_TYPE:
            return "Unknown block ID type, only blockhash and block height "
                   "allowed";
        case RpcErrorCode::BLOCKHASH_INVALID_SIZE:
            return "Invalid blockhash size, must be 32 bytes";
        case RpcErrorCode::BLOCK_NOT_FOUND:
            return "Block not found";
        case RpcErrorCode::BLOCK_DATA_CORRUPTED:
            return "Block data corrupted";
        default:
            return "Unknown error";
    }
}

class PluginRpcServer;

class PluginRpcWorker {
    PluginRpcWorkerState m_state;
    nng_aio *m_aio;
    nng_ctx m_ctx;
    PluginRpcServer *m_server;

    void HandleCallback();

public:
    PluginRpcWorker();

    void Init(nng_socket sock, PluginRpcServer *server);
    static void Callback(void *arg);
};

class PluginRpcServer {
    static const size_t NUM_WORKERS = 64;

    nng_socket m_sock;
    std::vector<PluginRpcWorker> m_workers;
    bool m_is_running;
    const Config &m_config;

    RpcErrorCode GetBlock(flatbuffers::FlatBufferBuilder &builder,
                          const PluginInterface::GetBlockRequest *request);
    RpcErrorCode
    GetBlockUndoData(flatbuffers::FlatBufferBuilder &builder,
                     const PluginInterface::GetBlockUndoDataRequest *request);
    RpcErrorCode GetMempool(flatbuffers::FlatBufferBuilder &builder,
                            const PluginInterface::GetMempoolRequest *request);

public:
    PluginRpcServer(const Config &config);
    RpcErrorCode HandleMsg(flatbuffers::FlatBufferBuilder &builder,
                           nng_msg *incoming_msg);
    void Serve(const std::string &rpc_url);
    void Shutdown() { m_is_running = false; }
};

PluginRpcServer::PluginRpcServer(const Config &config) : m_config(config) {
    m_is_running = false;
}

void PluginRpcServer::Serve(const std::string &rpc_url) {
    NNG_TRY(nng_rep0_open(&m_sock));
    std::cout << "NNG RPC server listening at " << rpc_url << std::endl;

    m_workers.resize(NUM_WORKERS);

    NNG_TRY(nng_listen(m_sock, rpc_url.c_str(), NULL, 0));

    for (PluginRpcWorker &worker : m_workers) {
        worker.Init(m_sock, this);
    }

    m_is_running = true;
    while (m_is_running) {
        nng_msleep(50);
    }
}

PluginRpcWorker::PluginRpcWorker() {
    m_state = PluginRpcWorkerState::UNINIT;
}

void PluginRpcWorker::Init(nng_socket sock, PluginRpcServer *server) {
    m_server = server;
    NNG_TRY(nng_aio_alloc(&m_aio, PluginRpcWorker::Callback, this));
    NNG_TRY(nng_ctx_open(&m_ctx, sock));
    m_state = PluginRpcWorkerState::RECV;
    nng_ctx_recv(m_ctx, m_aio);
}

void PluginRpcWorker::Callback(void *arg) {
    PluginRpcWorker *worker = (PluginRpcWorker *)arg;
    worker->HandleCallback();
}

void PluginRpcWorker::HandleCallback() {
    switch (m_state) {
        case PluginRpcWorkerState::UNINIT:
            std::cout << "Error: Worker in state UNINIT" << std::endl;
            break;
        case PluginRpcWorkerState::RECV: {
            NNG_TRY(nng_aio_result(m_aio));
            nng_msg *incoming_msg = nng_aio_get_msg(m_aio);
            flatbuffers::FlatBufferBuilder builder;
            RpcErrorCode error_code =
                m_server->HandleMsg(builder, incoming_msg);
            flatbuffers::FlatBufferBuilder result_builder(builder.GetSize() +
                                                          256);
            if (error_code == RpcErrorCode::NO_ERROR) {
                result_builder.Finish(PluginInterface::CreateRpcResult(
                    result_builder, true, 0, result_builder.CreateString(""),
                    result_builder.CreateVector(builder.GetBufferPointer(),
                                                builder.GetSize())));
            } else {
                result_builder.Finish(PluginInterface::CreateRpcResult(
                    result_builder, false, int32_t(error_code),
                    result_builder.CreateString(ErrorMsg(error_code))));
            }
            nng_msg *outgoing_msg;
            NNG_TRY(nng_msg_alloc(&outgoing_msg, result_builder.GetSize()));
            memcpy(nng_msg_body(outgoing_msg),
                   (void *)result_builder.GetBufferPointer(),
                   result_builder.GetSize());
            nng_aio_set_msg(m_aio, outgoing_msg);
            m_state = PluginRpcWorkerState::SEND;
            nng_ctx_send(m_ctx, m_aio);
            break;
        }
        case PluginRpcWorkerState::SEND: {
            NNG_TRY(nng_aio_result(m_aio));
            m_state = PluginRpcWorkerState::RECV;
            nng_ctx_recv(m_ctx, m_aio);
            break;
        }
    }
}

RpcErrorCode PluginRpcServer::HandleMsg(flatbuffers::FlatBufferBuilder &builder,
                                        nng_msg *incoming_msg) {
    flatbuffers::Verifier verifier((uint8_t *)nng_msg_body(incoming_msg),
                                   nng_msg_len(incoming_msg));
    if (!verifier.VerifyBuffer<PluginInterface::RpcCall>()) {
        return RpcErrorCode::INVALID_FLATBUFFER_ENCODING;
    }
    const PluginInterface::RpcCall *rpc =
        flatbuffers::GetRoot<PluginInterface::RpcCall>(
            nng_msg_body(incoming_msg));
    switch (rpc->rpc_type()) {
        case PluginInterface::RpcCallType_GetBlockRequest: {
            return GetBlock(builder, rpc->rpc_as_GetBlockRequest());
        }
        case PluginInterface::RpcCallType_GetBlockUndoDataRequest: {
            return GetBlockUndoData(builder,
                                    rpc->rpc_as_GetBlockUndoDataRequest());
        }
        case PluginInterface::RpcCallType_GetMempoolRequest: {
            return GetMempool(builder, rpc->rpc_as_GetMempoolRequest());
        }
        default:
            return RpcErrorCode::UNKNOWN_RPC_METHOD;
    }
}

template <typename T>
RpcErrorCode GetBlockIndex(const T *request, CBlockIndex *&block_index) {
    switch (request->block_id_type()) {
        case PluginInterface::BlockIdentifier_Height: {
            int32_t height = request->block_id_as_Height()->height();
            block_index = ::ChainActive().Tip()->GetAncestor(height);
            break;
        }
        case PluginInterface::BlockIdentifier_Blockhash: {
            const flatbuffers::Vector<uint8_t> *blockhash_fb =
                request->block_id_as_Blockhash()->blockhash();
            const std::vector<uint8_t> blockhash(blockhash_fb->begin(),
                                                 blockhash_fb->end());
            if (blockhash.size() != uint256().size()) {
                return RpcErrorCode::BLOCKHASH_INVALID_SIZE;
            }
            block_index = LookupBlockIndex(BlockHash(uint256(blockhash)));
            break;
        }
        default:
            return RpcErrorCode::BLOCK_ID_UNKNOWN_TYPE;
    }
    if (!block_index) {
        return RpcErrorCode::BLOCK_NOT_FOUND;
    }
    return RpcErrorCode::NO_ERROR;
}

flatbuffers::Offset<PluginInterface::Block>
CreateFbsBlock(flatbuffers::FlatBufferBuilder &builder, const CBlock &block) {
    CDataStream header(SER_NETWORK, PROTOCOL_VERSION);
    header << block.GetBlockHeader();
    std::vector<flatbuffers::Offset<PluginInterface::Tx>> txs_fbs;
    for (const CTransactionRef &tx : block.vtx) {
        CDataStream tx_ser(SER_NETWORK, PROTOCOL_VERSION);
        tx_ser << tx;
        txs_fbs.push_back(PluginInterface::CreateTx(
            builder,
            builder.CreateVector((uint8_t *)tx_ser.data(), tx_ser.size())));
    }
    return PluginInterface::CreateBlock(
        builder, builder.CreateVector((uint8_t *)header.data(), header.size()),
        builder
            .CreateVector<flatbuffers::Offset<PluginInterface::BlockMetadata>>(
                {}),
        builder.CreateVector(txs_fbs));
}

RpcErrorCode
PluginRpcServer::GetBlock(flatbuffers::FlatBufferBuilder &builder,
                          const PluginInterface::GetBlockRequest *request) {
    LOCK(cs_main);
    RpcErrorCode code;
    CBlockIndex *block_index;
    if ((code = GetBlockIndex(request, block_index)) !=
        RpcErrorCode::NO_ERROR) {
        return code;
    }
    CBlock block;
    if (!ReadBlockFromDisk(block, block_index,
                           m_config.GetChainParams().GetConsensus())) {
        return RpcErrorCode::BLOCK_DATA_CORRUPTED;
    }
    builder.Finish(PluginInterface::CreateGetBlockResponse(
        builder, CreateFbsBlock(builder, block)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode PluginRpcServer::GetBlockUndoData(
    flatbuffers::FlatBufferBuilder &builder,
    const PluginInterface::GetBlockUndoDataRequest *request) {
    LOCK(cs_main);
    RpcErrorCode code;
    CBlockIndex *block_index;
    if ((code = GetBlockIndex(request, block_index)) !=
        RpcErrorCode::NO_ERROR) {
        return code;
    }
    if (block_index->GetUndoPos().IsNull()) {
        // return empty list of coins
        builder.Finish(
            PluginInterface::CreateGetBlockUndoDataResponse(builder));
        return RpcErrorCode::NO_ERROR;
    }
    CBlockUndo blockundo;
    if (!UndoReadFromDisk(blockundo, block_index)) {
        return RpcErrorCode::BLOCK_DATA_CORRUPTED;
    }
    std::vector<flatbuffers::Offset<PluginInterface::Coin>> coins_fbs;
    coins_fbs.reserve(blockundo.vtxundo.size());
    for (const CTxUndo &txundo : blockundo.vtxundo) {
        for (const Coin &coin : txundo.vprevout) {
            coins_fbs.push_back(PluginInterface::CreateCoin(
                builder, coin.GetTxOut().nValue / SATOSHI,
                builder.CreateVector(coin.GetTxOut().scriptPubKey.data(),
                                     coin.GetTxOut().scriptPubKey.size()),
                coin.GetHeight(), coin.IsCoinBase()));
        }
    }
    builder.Finish(PluginInterface::CreateGetBlockUndoDataResponse(
        builder, builder.CreateVector(coins_fbs)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode
PluginRpcServer::GetMempool(flatbuffers::FlatBufferBuilder &builder,
                            const PluginInterface::GetMempoolRequest *request) {
    LOCK(g_mempool.cs);
    std::vector<flatbuffers::Offset<PluginInterface::MempoolTx>> txs_fbs;
    for (const CTxMemPoolEntry &entry : g_mempool.mapTx) {
        CDataStream tx_ser(SER_NETWORK, PROTOCOL_VERSION);
        tx_ser << entry.GetTx();
        txs_fbs.push_back(PluginInterface::CreateMempoolTx(
            builder,
            builder.CreateVector((uint8_t *)tx_ser.data(), tx_ser.size())));
    }
    builder.Finish(PluginInterface::CreateGetMempoolResponse(
        builder, builder.CreateVector(txs_fbs)));
    return RpcErrorCode::NO_ERROR;
}

class PluginPubServer final : public CValidationInterface {
public:
    void Listen(const std::string &rpc_url) {
        NNG_TRY(nng_pub0_open(&m_sock));
        NNG_TRY(nng_listen(m_sock, rpc_url.c_str(), NULL, 0));
        std::cout << "NNG pub server listening at " << rpc_url << std::endl;
        RegisterValidationInterface(this);
    }

private:
    nng_socket m_sock;

    void BroadcastMessage(const std::string msg_type,
                          const flatbuffers::FlatBufferBuilder &fbb) {
        std::vector<uint8_t> msg;
        msg.resize(12 + fbb.GetSize());
        memcpy(msg.data(), msg_type.data(), msg_type.size());
        memcpy(msg.data() + 12, fbb.GetBufferPointer(), fbb.GetSize());
        NNG_TRY(nng_send(m_sock, msg.data(), msg.size(), 0));
    }

    void UpdatedBlockTip(const CBlockIndex *pindexNew,
                         const CBlockIndex *pindexFork,
                         bool fInitialDownload) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateUpdatedBlockTip(
            fbb, fbb.CreateVector(pindexNew->GetBlockHash().begin(),
                                  pindexNew->GetBlockHash().size())));
        BroadcastMessage("updateblktip", fbb);
    }

    void TransactionAddedToMempool(const CTransactionRef &ptx) override {
        flatbuffers::FlatBufferBuilder fbb;
        CDataStream tx(SER_NETWORK, PROTOCOL_VERSION);
        tx << ptx;
        fbb.Finish(PluginInterface::CreateTransactionAddedToMempool(
            fbb, PluginInterface::CreateTx(
                     fbb, fbb.CreateVector((uint8_t *)tx.data(), tx.size()))));
        BroadcastMessage("mempooltxadd", fbb);
    }

    void TransactionRemovedFromMempool(const CTransactionRef &ptx) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateTransactionRemovedFromMempool(
            fbb, fbb.CreateVector(ptx->GetId().begin(), ptx->GetId().size())));
        BroadcastMessage("mempooltxrem", fbb);
    }

    void
    BlockConnected(const std::shared_ptr<const CBlock> &block,
                   const CBlockIndex *pindex,
                   const std::vector<CTransactionRef> &txnConflicted) override {
        flatbuffers::FlatBufferBuilder fbb;
        auto block_fb = CreateFbsBlock(fbb, *block);
        std::vector<PluginInterface::Txid> txs_conflicted;
        for (const CTransactionRef &conflicted_tx : txnConflicted) {
            PluginInterface::Txid txid;
            memcpy(txid.mutable_id()->Data(), conflicted_tx->GetId().begin(),
                   conflicted_tx->GetId().size());
            txs_conflicted.push_back(txid);
        }
        fbb.Finish(PluginInterface::CreateBlockConnectedDirect(
            fbb, block_fb, &txs_conflicted));
        BroadcastMessage("blkconnected", fbb);
    }

    void
    BlockDisconnected(const std::shared_ptr<const CBlock> &block) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateBlockDisconnected(
            fbb, fbb.CreateVector(block->GetHash().begin(),
                                  block->GetHash().size())));
        BroadcastMessage("blkdisconctd", fbb);
    }

    void ChainStateFlushed(const CBlockLocator &locator) override {
        if (locator.vHave.size() == 0) {
            return;
        }
        flatbuffers::FlatBufferBuilder fbb;
        BlockHash blockhash = locator.vHave[0];
        fbb.Finish(PluginInterface::CreateBlockDisconnected(
            fbb, fbb.CreateVector(blockhash.begin(), blockhash.size())));
        BroadcastMessage("chainstflush", fbb);
    }

    void BlockChecked(const CBlock &block,
                      const CValidationState &validation_state) override {
        flatbuffers::FlatBufferBuilder fbb;
        PluginInterface::BlockValidationModeState state;
        if (validation_state.IsValid()) {
            state = PluginInterface::BlockValidationModeState_Valid;
        } else if (validation_state.IsInvalid()) {
            state = PluginInterface::BlockValidationModeState_Invalid;
        } else {
            state = PluginInterface::BlockValidationModeState_Error;
        }
        fbb.Finish(PluginInterface::CreateBlockChecked(
            fbb,
            fbb.CreateVector(block.GetHash().begin(), block.GetHash().size()),
            PluginInterface::CreateBlockValidationState(
                fbb, state,
                fbb.CreateString(validation_state.GetRejectReason()),
                fbb.CreateString(validation_state.GetDebugMessage()))));
        BroadcastMessage("blockchecked", fbb);
    }

    void NewPoWValidBlock(const CBlockIndex *pindex,
                          const std::shared_ptr<const CBlock> &block) override {
        flatbuffers::FlatBufferBuilder fbb;
        CDataStream header(SER_NETWORK, PROTOCOL_VERSION);
        header << block->GetBlockHeader();
        fbb.Finish(PluginInterface::CreateUpdatedBlockTip(
            fbb, fbb.CreateVector((uint8_t *)header.data(), header.size())));
        BroadcastMessage("newpowvldblk", fbb);
    }
};

std::thread g_rpc_thread;
bool has_rpc = false;
std::unique_ptr<PluginRpcServer> g_rpc_server;
std::unique_ptr<PluginPubServer> g_pub_server;

void RunServer() {
    std::string rpc_url = gArgs.GetArg("-pluginrpcurl", "");
    if (rpc_url.size() > 0) {
        has_rpc = true;
        g_rpc_server = std::make_unique<PluginRpcServer>(GetConfig());
        g_rpc_server->Serve(rpc_url);
    }
}

void StartPluginNng() {
    g_rpc_thread = std::thread(RunServer);
    std::string pub_url = gArgs.GetArg("-pluginpuburl", "");
    if (pub_url.size() > 0) {
        g_pub_server = std::make_unique<PluginPubServer>();
        g_pub_server->Listen(pub_url);
    }
}

void StopPluginNng() {
    if (has_rpc) {
        g_rpc_server->Shutdown();
    }
    g_rpc_thread.join();
}
