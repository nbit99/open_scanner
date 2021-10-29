package dto

import (
	"github.com/blocktree/go-openw-server/open_base/model"
	"github.com/blocktree/openwallet/v2/openwallet"
)

type BatchCreateAddressResp struct {
	Address []*model.OwAddress
}

type PublicKeyToAddressResp struct {
	Address string
}

type CreateRawTransactionResp struct {
	RawTx *openwallet.RawTransaction
}

type SubmitRawTransactionResp struct {
	Tx   *openwallet.Transaction
	TxID string
}

type CreateSummaryRawTransactionReqResp struct {
	RawTxs []*SmayTx
}

type SmayTx struct {
	Tx    *openwallet.RawTransaction
	Error map[string]interface{}
}

type GetBalanceByAddressResp struct {
	Balance     []*openwallet.Balance
	BalanceType int64 // 0.地址模型 1.账户模型
}

type GetTokenBalanceByAddressResp struct {
	Balance     []*openwallet.TokenBalance
	BalanceType int64 // 0.地址模型 1.账户模型
}

type GetRawTransactionFeeRateResp struct {
	FeeRate string
	Unit    string
}

type RescannerHeightResp struct {
}

type RescannerOneHeightResp struct {
}

type GetBalanceTypeResp struct {
	BalanceType int64
}

type OnOffScannerResp struct {
}

type VerifyAddressResp struct {
	Result bool
}

// 调用智能合约ABI方法  callSmartContractABI
type CallSmartContractABIResp struct {
	Result *openwallet.SmartContractCallResult
}

// 创建智能合约交易单  createSmartContractTrade
type CreateSmartContractTradeResp struct {
	Rawtx *openwallet.SmartContractRawTransaction
}

// 广播转账交易订单 submitSmartContractTrade
type SubmitSmartContractTradeResp struct {
	Receipt *openwallet.SmartContractReceipt
}
