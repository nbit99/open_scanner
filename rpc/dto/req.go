package dto

import (
	"github.com/blocktree/openwallet/v2/openwallet"
	"github.com/nbit99/open_base/major"
	"github.com/nbit99/open_base/model"
)

type BatchCreateAddressReq struct {
	Symbol   string
	Account  *model.OwAccount
	Conf     *major.Network
	Count    int
	Worksize int
}

type PublicKeyToAddressReq struct {
	Symbol    string
	PublicKey string
	IsTestnet bool
}

type CreateRawTransactionReq struct {
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	RawTx     *openwallet.RawTransaction
}

type SubmitRawTransactionReq struct {
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	RawTx     *openwallet.RawTransaction
}

type CreateSummaryRawTransactionReq struct {
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	Smrtx     *openwallet.SummaryRawTransaction
}

type GetBalanceByAddressReq struct {
	Symbol    string
	Address   []string
	AccountID string
}

type GetTokenBalanceByAddressReq struct {
	Symbol    string
	Contract  openwallet.SmartContract
	Address   []string
	AccountID string
}

type GetRawTransactionFeeRateReq struct {
	Symbol string
}

type RescannerHeightReq struct {
	Symbol  string
	Height  int64
	IsForce int64
}

type RescannerOneHeightReq struct {
	Symbol  string
	Height  int64
	IsForce int64
}

type GetBalanceTypeReq struct {
	Symbol string
}

type OnOffScannerReq struct {
	Symbol string
	OnOff  int64 // 1.开 2.关
}

type VerifyAddressReq struct {
	Symbol  string
	Address string
}

// 调用智能合约ABI方法  callSmartContractABI
type CallSmartContractABIReq struct {
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	Rawtx     *openwallet.SmartContractRawTransaction
}

// 创建智能合约交易单  createSmartContractTrade
type CreateSmartContractTradeReq struct {
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	Rawtx     *openwallet.SmartContractRawTransaction
}

// 广播转账交易订单 submitSmartContractTrade
type SubmitSmartContractTradeReq struct {
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	Rawtx     *openwallet.SmartContractRawTransaction
}
