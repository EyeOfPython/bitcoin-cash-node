// Copyright (c) 2017-2018 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <coins.h>
#include <consensus/consensus.h>
#include <consensus/validation.h>
#include <primitives/transaction.h>
#include <version.h>

#include <unordered_set>

static bool CheckTransactionCommon(const CTransaction &tx,
                                   CValidationState &state) {
    // Basic checks that don't depend on any context
    if (tx.vin.empty()) {
        return state.DoS(10, false, REJECT_INVALID, "bad-txns-vin-empty");
    }

    if (tx.vout.empty()) {
        return state.DoS(10, false, REJECT_INVALID, "bad-txns-vout-empty");
    }

    // Size limit
    if (::GetSerializeSize(tx, PROTOCOL_VERSION) > MAX_TX_SIZE) {
        return state.DoS(100, false, REJECT_INVALID, "bad-txns-oversize");
    }

    // Check for negative or overflow output values
    Amount nValueOut = Amount::zero();
    for (const auto &txout : tx.vout) {
        if (txout.nValue < Amount::zero()) {
            return state.DoS(100, false, REJECT_INVALID,
                             "bad-txns-vout-negative");
        }

        if (txout.nValue > MAX_MONEY) {
            return state.DoS(100, false, REJECT_INVALID,
                             "bad-txns-vout-toolarge");
        }

        nValueOut += txout.nValue;
        if (!MoneyRange(nValueOut)) {
            return state.DoS(100, false, REJECT_INVALID,
                             "bad-txns-txouttotal-toolarge");
        }
    }

    return true;
}

bool CheckCoinbase(const CTransaction &tx, CValidationState &state) {
    if (!tx.IsCoinBase()) {
        return state.DoS(100, false, REJECT_INVALID, "bad-cb-missing", false,
                         "first tx is not coinbase");
    }

    if (!CheckTransactionCommon(tx, state)) {
        // CheckTransactionCommon fill in the state.
        return false;
    }

    if (tx.vin[0].scriptSig.size() < 2 ||
        tx.vin[0].scriptSig.size() > MAX_COINBASE_SCRIPTSIG_SIZE) {
        return state.DoS(100, false, REJECT_INVALID, "bad-cb-length");
    }

    return true;
}

bool CheckRegularTransaction(const CTransaction &tx, CValidationState &state) {
    if (tx.IsCoinBase()) {
        return state.DoS(100, false, REJECT_INVALID, "bad-tx-coinbase");
    }

    if (!CheckTransactionCommon(tx, state)) {
        // CheckTransactionCommon fill in the state.
        return false;
    }

    // Creating and filling an unordered_set is O(n), but simply checking inputs is O(n^2). However, the unordered_set
    // requires memory allocations, which are significantly slower for small transactions. The crossover point appears
    // to be a vin.size() of about 300.
    if (tx.vin.size() < 300) {
        for (size_t i=0; i < tx.vin.size(); ++i) {
            if (tx.vin[i].prevout.IsNull()) {
                return state.DoS(10, false, REJECT_INVALID,
                                 "bad-txns-prevout-null");
            }
            for (size_t j=i+1; j < tx.vin.size(); ++j) {
                if (tx.vin[i].prevout == tx.vin[j].prevout) {
                    return state.DoS(100, false, REJECT_INVALID, "bad-txns-inputs-duplicate");
                }
            }
        }
    } else {
        std::unordered_set<COutPoint, SaltedOutpointHasher> vInOutPoints;
        for (const auto &txin : tx.vin) {
            if (txin.prevout.IsNull()) {
                return state.DoS(10, false, REJECT_INVALID,
                                 "bad-txns-prevout-null");
            }

            if (!vInOutPoints.insert(txin.prevout).second) {
                return state.DoS(100, false, REJECT_INVALID,
                                 "bad-txns-inputs-duplicate");
            }
        }
    }
    return true;
}
