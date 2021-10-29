package service

import (
	"github.com/blocktree/go-openw-server/open_scanner/rpc/dto"
)

type WalletApiService interface {
	// 批量创建地址
	BatchCreateAddress(req *dto.BatchCreateAddressReq, resp *dto.BatchCreateAddressResp) error
	// 公钥转成地址
	PublicKeyToAddress(req *dto.PublicKeyToAddressReq, resp *dto.PublicKeyToAddressResp) error
	// 创建交易单
	CreateRawTransaction(req *dto.CreateRawTransactionReq, resp *dto.CreateRawTransactionResp) error
	// 广播交易单
	SubmitRawTransaction(req *dto.SubmitRawTransactionReq, resp *dto.SubmitRawTransactionResp) error
	// 汇总交易单
	CreateSummaryRawTransaction(req *dto.CreateSummaryRawTransactionReq, resp *dto.CreateSummaryRawTransactionReqResp) error
	// 获取地址余额
	GetBalanceByAddress(req *dto.GetBalanceByAddressReq, resp *dto.GetBalanceByAddressResp) error
	// 获取token地址余额
	GetTokenBalanceByAddress(req *dto.GetTokenBalanceByAddressReq, resp *dto.GetTokenBalanceByAddressResp) error
	// 获取交易手续费
	GetRawTransactionFeeRate(req *dto.GetRawTransactionFeeRateReq, resp *dto.GetRawTransactionFeeRateResp) error
	// 设置重扫高度
	RescannerHeight(req *dto.RescannerHeightReq, resp *dto.RescannerHeightResp) error
	// 设置重扫单个高度
	RescannerOneHeight(req *dto.RescannerOneHeightReq, resp *dto.RescannerOneHeightResp) error
	// 获取余额模型类型
	GetBalanceType(req *dto.GetBalanceTypeReq, resp *dto.GetBalanceTypeResp) error
	// 开启/暂停扫块器
	OnOffScanner(req *dto.OnOffScannerReq, resp *dto.OnOffScannerResp) error
	// 校验地址
	VerifyAddress(req *dto.VerifyAddressReq, resp *dto.VerifyAddressResp) error
	// 调用智能合约ABI方法
	CallSmartContractABI(req *dto.CallSmartContractABIReq, resp *dto.CallSmartContractABIResp) error
	// 创建智能合约交易单
	CreateSmartContractTrade(req *dto.CreateSmartContractTradeReq, resp *dto.CreateSmartContractTradeResp) error
	// 广播转账交易订单
	SubmitSmartContractTrade(req *dto.SubmitSmartContractTradeReq, resp *dto.SubmitSmartContractTradeResp) error
}
