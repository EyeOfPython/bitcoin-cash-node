#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/supplemental/util/platform.h>

#include <thread>

#include <chain.h>
#include <chainparams.h>
#include <config.h>
#include <consensus/validation.h>
#include <logging.h>
#include <node/transaction.h>
#include <rpc/protocol.h>
#include <streams.h>
#include <sync.h>
#include <txmempool.h>
#include <undo.h>
#include <util/system.h>
#include <validation.h>
#include <validationinterface.h>

#include "plugin_interface_generated.h"

enum class PluginRpcWorkerState {
    UNINIT,
    RECV,
    SEND,
};

class PluginRpcServer;

#define NNG_TRY_FN(call, abort_fn)                                             \
    do {                                                                       \
        int rv = (call);                                                       \
        if (rv != 0) {                                                         \
            LogPrintf("NNG Error: %s (at %s:%d: %s)\n", nng_strerror(rv),      \
                      __FILE__, __LINE__, #call);                              \
            abort_fn;                                                          \
        }                                                                      \
    } while (false)

#define NNG_TRY(call) NNG_TRY_FN(call, return )
#define NNG_TRY_ABORT(call) NNG_TRY_FN(call, exit(0))

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
    GetBlockRange(flatbuffers::FlatBufferBuilder &builder,
                  const PluginInterface::GetBlockRangeRequest *request);
    RpcErrorCode
    GetBlockUndoData(flatbuffers::FlatBufferBuilder &builder,
                     const PluginInterface::GetBlockUndoDataRequest *request);
    RpcErrorCode GetMempool(flatbuffers::FlatBufferBuilder &builder,
                            const PluginInterface::GetMempoolRequest *request);
    RpcErrorCode
    GetBlockchainInfo(flatbuffers::FlatBufferBuilder &builder,
                      const PluginInterface::GetBlockchainInfoRequest *request);
    RpcErrorCode
    SubmitBlock(flatbuffers::FlatBufferBuilder &builder,
                const PluginInterface::SubmitBlockRequest *request);
    RpcErrorCode SubmitTx(flatbuffers::FlatBufferBuilder &builder,
                          const PluginInterface::SubmitTxRequest *request);

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
    NNG_TRY_ABORT(nng_rep0_open(&m_sock));
    LogPrintf("Plugin interface: NNG RPC server listening at %s\n", rpc_url);

    m_workers.resize(NUM_WORKERS);

    NNG_TRY_ABORT(nng_listen(m_sock, rpc_url.c_str(), NULL, 0));

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
    NNG_TRY_ABORT(nng_aio_alloc(&m_aio, PluginRpcWorker::Callback, this));
    NNG_TRY_ABORT(nng_ctx_open(&m_ctx, sock));
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
            LogPrintf("Error: Worker in state UNINIT\n");
            break;
        case PluginRpcWorkerState::RECV: {
            NNG_TRY(nng_aio_result(m_aio));
            nng_msg *incoming_msg = nng_aio_get_msg(m_aio);
            flatbuffers::FlatBufferBuilder fbb;
            RpcErrorCode error_code = m_server->HandleMsg(fbb, incoming_msg);
            flatbuffers::FlatBufferBuilder result_fbb(fbb.GetSize() + 256);
            if (error_code == RpcErrorCode::NO_ERROR) {
                result_fbb.Finish(PluginInterface::CreateRpcResult(
                    result_fbb, true, 0, result_fbb.CreateString(""),
                    result_fbb.CreateVector(fbb.GetBufferPointer(),
                                            fbb.GetSize())));
            } else {
                result_fbb.Finish(PluginInterface::CreateRpcResult(
                    result_fbb, false, int32_t(error_code),
                    result_fbb.CreateString(ErrorMsg(error_code))));
            }
            nng_msg *outgoing_msg;
            NNG_TRY(nng_msg_alloc(&outgoing_msg, result_fbb.GetSize()));
            memcpy(nng_msg_body(outgoing_msg),
                   (void *)result_fbb.GetBufferPointer(), result_fbb.GetSize());
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

RpcErrorCode PluginRpcServer::HandleMsg(flatbuffers::FlatBufferBuilder &fbb,
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
            return GetBlock(fbb, rpc->rpc_as_GetBlockRequest());
        }
        case PluginInterface::RpcCallType_GetBlockRangeRequest: {
            return GetBlockRange(fbb, rpc->rpc_as_GetBlockRangeRequest());
        }
        case PluginInterface::RpcCallType_GetBlockUndoDataRequest: {
            return GetBlockUndoData(fbb, rpc->rpc_as_GetBlockUndoDataRequest());
        }
        case PluginInterface::RpcCallType_GetMempoolRequest: {
            return GetMempool(fbb, rpc->rpc_as_GetMempoolRequest());
        }
        case PluginInterface::RpcCallType_GetBlockchainInfoRequest: {
            return GetBlockchainInfo(fbb,
                                     rpc->rpc_as_GetBlockchainInfoRequest());
        }
        case PluginInterface::RpcCallType_SubmitBlockRequest: {
            return SubmitBlock(fbb, rpc->rpc_as_SubmitBlockRequest());
        }
        case PluginInterface::RpcCallType_SubmitTxRequest: {
            return SubmitTx(fbb, rpc->rpc_as_SubmitTxRequest());
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
        case PluginInterface::BlockIdentifier_Hash: {
            const flatbuffers::Vector<uint8_t> *blockhash_fb =
                request->block_id_as_Hash()->hash();
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

flatbuffers::Offset<PluginInterface::BlockHash>
CreateFbsBlockHash(flatbuffers::FlatBufferBuilder &fbb, const BlockHash &hash) {
    return PluginInterface::CreateBlockHash(
        fbb, fbb.CreateVector(hash.begin(), hash.size()));
}

flatbuffers::Offset<PluginInterface::BlockHeader>
CreateFbsBlockHeader(flatbuffers::FlatBufferBuilder &fbb,
                     const CBlockHeader &header) {
    CDataStream raw_header(SER_NETWORK, PROTOCOL_VERSION);
    raw_header << header;
    return PluginInterface::CreateBlockHeader(
        fbb, fbb.CreateVector((uint8_t *)raw_header.data(), raw_header.size()),
        CreateFbsBlockHash(fbb, header.GetHash()),
        CreateFbsBlockHash(fbb, header.hashPrevBlock), header.nBits,
        header.nTime);
}

flatbuffers::Offset<PluginInterface::TxId>
CreateFbsTxId(flatbuffers::FlatBufferBuilder &fbb, const TxId &txid) {
    return PluginInterface::CreateTxId(
        fbb, fbb.CreateVector(txid.begin(), txid.size()));
}

flatbuffers::Offset<PluginInterface::Tx>
CreateFbsTx(flatbuffers::FlatBufferBuilder &fbb, const CTransactionRef &tx) {
    CDataStream tx_ser(SER_NETWORK, PROTOCOL_VERSION);
    tx_ser << tx;
    return PluginInterface::CreateTx(
        fbb, CreateFbsTxId(fbb, tx->GetId()),
        fbb.CreateVector((uint8_t *)tx_ser.data(), tx_ser.size()));
}

flatbuffers::Offset<PluginInterface::Coin>
CreateFbsCoin(flatbuffers::FlatBufferBuilder &fbb, const Coin &coin) {
    return PluginInterface::CreateCoin(
        fbb, coin.GetTxOut().nValue / SATOSHI,
        fbb.CreateVector(coin.GetTxOut().scriptPubKey.data(),
                         coin.GetTxOut().scriptPubKey.size()),
        coin.GetHeight(), coin.IsCoinBase());
}

flatbuffers::Offset<PluginInterface::Block>
CreateFbsBlock(flatbuffers::FlatBufferBuilder &fbb, const CBlock &block) {
    std::vector<flatbuffers::Offset<PluginInterface::Tx>> txs_fbs;
    for (const CTransactionRef &tx : block.vtx) {
        txs_fbs.push_back(CreateFbsTx(fbb, tx));
    }
    return PluginInterface::CreateBlock(
        fbb, CreateFbsBlockHeader(fbb, block.GetBlockHeader()),
        fbb.CreateVector<flatbuffers::Offset<PluginInterface::BlockMetadata>>(
            {}),
        fbb.CreateVector(txs_fbs));
}

RpcErrorCode
PluginRpcServer::GetBlock(flatbuffers::FlatBufferBuilder &fbb,
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
    fbb.Finish(PluginInterface::CreateGetBlockResponse(
        fbb, CreateFbsBlock(fbb, block)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode PluginRpcServer::GetBlockRange(
    flatbuffers::FlatBufferBuilder &fbb,
    const PluginInterface::GetBlockRangeRequest *request) {
    LOCK(cs_main);

    const int32_t chain_height = ::ChainActive().Height();
    const int32_t start_height = request->start_height();
    uint32_t num_blocks = request->num_blocks();
    int32_t end_height = start_height + num_blocks - 1;
    if (end_height > chain_height) {
        end_height = chain_height;
        num_blocks = end_height - start_height;
    }

    CBlockIndex *block_index = nullptr;
    if (start_height >= 0) {
        block_index = ::ChainActive().Tip()->GetAncestor(end_height);
    } else {
        num_blocks = 0;
    }
    std::vector<flatbuffers::Offset<PluginInterface::Block>> blocks_fbs(
        num_blocks);
    for (auto block_fbs = blocks_fbs.rbegin();
         block_fbs != blocks_fbs.rend() && block_index != nullptr;
         ++block_fbs) {
        CBlock block;
        if (!ReadBlockFromDisk(block, block_index,
                               m_config.GetChainParams().GetConsensus())) {
            return RpcErrorCode::BLOCK_DATA_CORRUPTED;
        }
        *block_fbs = CreateFbsBlock(fbb, block);
        block_index = block_index->pprev;
    }
    fbb.Finish(PluginInterface::CreateGetBlockRangeResponse(
        fbb, fbb.CreateVector(blocks_fbs)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode PluginRpcServer::GetBlockUndoData(
    flatbuffers::FlatBufferBuilder &fbb,
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
        fbb.Finish(PluginInterface::CreateGetBlockUndoDataResponse(fbb));
        return RpcErrorCode::NO_ERROR;
    }
    CBlockUndo blockundo;
    if (!UndoReadFromDisk(blockundo, block_index)) {
        return RpcErrorCode::BLOCK_DATA_CORRUPTED;
    }
    std::vector<flatbuffers::Offset<PluginInterface::TxUndo>> txundo_fbs;
    std::vector<std::vector<flatbuffers::Offset<PluginInterface::Coin>>>
        txcoins_fbs;
    txundo_fbs.reserve(blockundo.vtxundo.size());
    txcoins_fbs.reserve(blockundo.vtxundo.size());
    for (const CTxUndo &txundo : blockundo.vtxundo) {
        std::vector<flatbuffers::Offset<PluginInterface::Coin>> coins_fbs;
        coins_fbs.reserve(txundo.vprevout.size());
        for (const Coin &coin : txundo.vprevout) {
            coins_fbs.push_back(CreateFbsCoin(fbb, coin));
        }
        txcoins_fbs.push_back(std::move(coins_fbs));
    }
    for (const auto &coin_fbs : txcoins_fbs) {
        txundo_fbs.push_back(
            PluginInterface::CreateTxUndo(fbb, fbb.CreateVector(coin_fbs)));
    }
    fbb.Finish(PluginInterface::CreateGetBlockUndoDataResponse(
        fbb, fbb.CreateVector(txundo_fbs)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode
PluginRpcServer::GetMempool(flatbuffers::FlatBufferBuilder &fbb,
                            const PluginInterface::GetMempoolRequest *request) {
    LOCK(g_mempool.cs);
    std::vector<flatbuffers::Offset<PluginInterface::MempoolTx>> txs_fbs;
    for (const CTxMemPoolEntry &entry : g_mempool.mapTx) {
        txs_fbs.push_back(PluginInterface::CreateMempoolTx(
            fbb, CreateFbsTx(fbb, entry.GetSharedTx())));
    }
    fbb.Finish(PluginInterface::CreateGetMempoolResponse(
        fbb, fbb.CreateVector(txs_fbs)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode PluginRpcServer::GetBlockchainInfo(
    flatbuffers::FlatBufferBuilder &fbb,
    const PluginInterface::GetBlockchainInfoRequest *request) {
    LOCK(cs_main);
    const CBlockIndex *tip = ::ChainActive().Tip();

    fbb.Finish(PluginInterface::CreateGetBlockchainInfoResponse(
        fbb, fbb.CreateString(m_config.GetChainParams().NetworkIDString()),
        ::ChainActive().Height(), CreateFbsBlockHash(fbb, tip->GetBlockHash()),
        fPruneMode, IsInitialBlockDownload(),
        GuessVerificationProgress(Params().TxData(), tip)));
    return RpcErrorCode::NO_ERROR;
}

class SubmitBlock_StateCatcher : public CValidationInterface {
public:
    uint256 hash;
    bool found;
    CValidationState state;

    explicit SubmitBlock_StateCatcher(const uint256 &hashIn)
        : hash(hashIn), found(false), state() {}

protected:
    void BlockChecked(const CBlock &block,
                      const CValidationState &stateIn) override {
        if (block.GetHash() != hash) {
            return;
        }

        found = true;
        state = stateIn;
    }
};

RpcErrorCode PluginRpcServer::SubmitBlock(
    flatbuffers::FlatBufferBuilder &fbb,
    const PluginInterface::SubmitBlockRequest *request) {
    std::shared_ptr<CBlock> blockptr = std::make_shared<CBlock>();
    CBlock &block = *blockptr;
    std::vector<uint8_t> blockData(request->raw_block()->begin(),
                                   request->raw_block()->end());
    CDataStream ssBlock(blockData, SER_NETWORK, PROTOCOL_VERSION);
    try {
        ssBlock >> block;
    } catch (const std::exception &) {
        fbb.Finish(PluginInterface::CreateSubmitBlockResponse(
            fbb, false, fbb.CreateString("invalid"),
            fbb.CreateString("Block decode failed")));
        return RpcErrorCode::NO_ERROR;
    }

    if (block.vtx.empty() || !block.vtx[0]->IsCoinBase()) {
        fbb.Finish(PluginInterface::CreateSubmitBlockResponse(
            fbb, false, fbb.CreateString("invalid"),
            fbb.CreateString("Block does not start with a coinbase")));
        return RpcErrorCode::NO_ERROR;
    }

    const BlockHash hash = block.GetHash();
    {
        LOCK(cs_main);
        const CBlockIndex *pindex = LookupBlockIndex(hash);
        std::string state = "";
        if (pindex) {
            if (pindex->IsValid(BlockValidity::SCRIPTS)) {
                state = "duplicate";
            }
            if (pindex->nStatus.isInvalid()) {
                state = "duplicate-invalid";
            }
        }
        if (!state.empty()) {
            fbb.Finish(PluginInterface::CreateSubmitBlockResponse(
                fbb, false, fbb.CreateString(state),
                fbb.CreateString("Block already exists")));
            return RpcErrorCode::NO_ERROR;
        }
    }

    bool new_block;
    SubmitBlock_StateCatcher state_catcher(block.GetHash());
    RegisterValidationInterface(&state_catcher);
    bool accepted = ProcessNewBlock(m_config, blockptr,
                                    /* fForceProcessing */ true,
                                    /* fNewBlock */ &new_block);
    UnregisterValidationInterface(&state_catcher);
    std::string state = "";
    std::string msg = "";
    bool is_accepted = false;
    if (!new_block && accepted) {
        state = "duplicate";
    }

    if (state.empty() && !state_catcher.found) {
        is_accepted = true;
        state = "inconclusive";
    }

    if (state.empty()) {
        if (state_catcher.state.IsValid()) {
            is_accepted = true;
            state = "valid";
        } else if (state_catcher.state.IsError()) {
            is_accepted = false;
            state = "error";
            msg = FormatStateMessage(state_catcher.state);
        } else if (state_catcher.state.IsInvalid()) {
            is_accepted = false;
            state = "invalid";
            msg = state_catcher.state.GetRejectReason();
            if (msg.empty()) {
                msg = "rejected";
            }
        } else {
            is_accepted = false;
            state = "unreachable";
        }
    }
    fbb.Finish(PluginInterface::CreateSubmitBlockResponse(
        fbb, is_accepted, fbb.CreateString(state), fbb.CreateString(msg)));
    return RpcErrorCode::NO_ERROR;
}

RpcErrorCode
PluginRpcServer::SubmitTx(flatbuffers::FlatBufferBuilder &fbb,
                          const PluginInterface::SubmitTxRequest *request) {
    std::vector<uint8_t> txdata(request->raw_tx()->begin(),
                                request->raw_tx()->end());
    CDataStream ss(txdata, SER_NETWORK, PROTOCOL_VERSION);
    CMutableTransaction mtx;
    bool parse_fail = true;
    try {
        ss >> mtx;
        if (ss.eof()) {
            parse_fail = false;
        }
    } catch (const std::exception &e) {
        // Fall through.
    }
    if (parse_fail) {
        fbb.Finish(PluginInterface::CreateSubmitTxResponse(
            fbb, false, fbb.CreateString("tx-decode-fail")));
        return RpcErrorCode::NO_ERROR;
    }
    CTransactionRef tx(MakeTransactionRef(std::move(mtx)));
    TxId txid;
    try {
        txid = BroadcastTransaction(m_config, tx, request->allow_high_fees());
    } catch (JSONRPCError e) {
        fbb.Finish(PluginInterface::CreateSubmitTxResponse(
            fbb, false, fbb.CreateString(e.message)));
        return RpcErrorCode::NO_ERROR;
    }
    fbb.Finish(PluginInterface::CreateSubmitTxResponse(
        fbb, true, fbb.CreateString(""), CreateFbsTxId(fbb, txid)));
    return RpcErrorCode::NO_ERROR;
}

class PluginPubServer final : public CValidationInterface {
public:
    void Listen(const std::string &pub_url) {
        NNG_TRY_ABORT(nng_pub0_open(&m_sock));
        NNG_TRY_ABORT(nng_listen(m_sock, pub_url.c_str(), NULL, 0));
        LogPrintf("Plugin interface: NNG pub server listening at %s\n",
                  pub_url);
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
            fbb, CreateFbsBlockHash(fbb, pindexNew->GetBlockHash())));
        BroadcastMessage("updateblktip", fbb);
    }

    void TransactionAddedToMempool(const CTransactionRef &ptx) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateTransactionAddedToMempool(
            fbb, PluginInterface::CreateMempoolTx(fbb, CreateFbsTx(fbb, ptx))));
        BroadcastMessage("mempooltxadd", fbb);
    }

    void TransactionRemovedFromMempool(const CTransactionRef &ptx) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateTransactionRemovedFromMempool(
            fbb, CreateFbsTxId(fbb, ptx->GetId())));
        BroadcastMessage("mempooltxrem", fbb);
    }

    void
    BlockConnected(const std::shared_ptr<const CBlock> &block,
                   const CBlockIndex *pindex,
                   const std::vector<CTransactionRef> &txnConflicted) override {
        flatbuffers::FlatBufferBuilder fbb;
        auto block_fb = CreateFbsBlock(fbb, *block);
        std::vector<flatbuffers::Offset<PluginInterface::TxId>> txs_conflicted;
        for (const CTransactionRef &conflicted_tx : txnConflicted) {
            txs_conflicted.push_back(
                CreateFbsTxId(fbb, conflicted_tx->GetId()));
        }
        fbb.Finish(PluginInterface::CreateBlockConnectedDirect(
            fbb, block_fb, &txs_conflicted));
        BroadcastMessage("blkconnected", fbb);
    }

    void
    BlockDisconnected(const std::shared_ptr<const CBlock> &block) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateBlockDisconnected(
            fbb, CreateFbsBlockHash(fbb, block->GetHash())));
        BroadcastMessage("blkdisconctd", fbb);
    }

    void ChainStateFlushed(const CBlockLocator &locator) override {
        if (locator.vHave.size() == 0) {
            return;
        }
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateBlockDisconnected(
            fbb, CreateFbsBlockHash(fbb, locator.vHave[0])));
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
            fbb, CreateFbsBlockHash(fbb, block.GetHash()),
            PluginInterface::CreateBlockValidationState(
                fbb, state,
                fbb.CreateString(validation_state.GetRejectReason()),
                fbb.CreateString(validation_state.GetDebugMessage()))));
        BroadcastMessage("blockchecked", fbb);
    }

    void NewPoWValidBlock(const CBlockIndex *pindex,
                          const std::shared_ptr<const CBlock> &block) override {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.Finish(PluginInterface::CreateNewPoWValidBlock(
            fbb, CreateFbsBlockHeader(fbb, block->GetBlockHeader())));
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
