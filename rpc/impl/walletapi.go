package impl

import (
	"encoding/hex"
	"fmt"
	"github.com/nbit99/openwallet/v2/openwallet"
	"github.com/godaddy-x/jorm/sqlc"
	"github.com/godaddy-x/jorm/sqld"
	"github.com/godaddy-x/jorm/util"
	"github.com/nbit99/open_base/common"
	"github.com/nbit99/open_base/model"
	"github.com/nbit99/open_scanner"
	"github.com/nbit99/open_scanner/rpc/dto"
	"time"
)

type WalletApiService struct {
}

func (self *WalletApiService) BatchCreateAddress(req *dto.BatchCreateAddressReq, resp *dto.BatchCreateAddressResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	account := req.Account
	addrArr, err := openwallet.BatchCreateAddressByAccount(account.ToAssetsAccount(), assetsMgr, int64(req.Count), req.Worksize)
	if err != nil {
		return err
	}
	owAddrs := make([]*model.OwAddress, 0)
	for _, a := range addrArr {
		newAddr := &model.OwAddress{
			AppID:            account.AppID,
			WalletID:         account.WalletID,
			AccountID:        account.AccountID,
			Symbol:           account.Symbol,
			AddrIndex:        int64(a.Index),
			Address:          a.Address,
			Balance:          "0",
			ConfirmBalance:   "0",
			UnconfirmBalance: "0",
			IsMemo:           0,
			Memo:             "",
			WatchOnly:        0,
			PublicKey:        a.PublicKey,
			CreatedAt:        util.Time(),
			Alias:            "",
			Tag:              "",
			Num:              0,
			HdPath:           a.HDPath,
			Batchno:          "0",
			IsChange:         req.Conf.AddrIsChange,
			Applytime:        util.Time(),
			Ctime:            util.Time(),
			Utime:            util.Time(),
			Dealstate:        2,
			State:            1,
		}
		owAddrs = append(owAddrs, newAddr)
	}
	resp.Address = owAddrs
	return nil
}

func (self *WalletApiService) PublicKeyToAddress(req *dto.PublicKeyToAddressReq, resp *dto.PublicKeyToAddressResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	b, err := hex.DecodeString(req.PublicKey)
	if err != nil {
		return util.Error(util.AddStr("公钥[", req.PublicKey, "]无效"))
	}
	addr, err := assetsMgr.GetAddressDecode().PublicKeyToAddress(b, req.IsTestnet)
	if err != nil {
		return util.Error(util.AddStr("公钥[", req.PublicKey, "]导出地址失败"))
	}
	resp.Address = addr
	return nil
}

func (self *WalletApiService) CreateRawTransaction(req *dto.CreateRawTransactionReq, resp *dto.CreateRawTransactionResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	txdecoder := assetsMgr.GetTransactionDecoder()
	if txdecoder == nil {
		return util.Error("txdecoder [", req.Symbol, "] is nil")
	}
	wrapper := open_scanner.NewWrapper(req.AppID, req.WalletID, req.AccountID, req.Symbol)
	rawtx := req.RawTx
	if err := txdecoder.CreateRawTransaction(wrapper, rawtx); err != nil {
		return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]创建交易单失败"))
	}
	resp.RawTx = rawtx
	return nil
}

func (self *WalletApiService) SubmitRawTransaction(req *dto.SubmitRawTransactionReq, resp *dto.SubmitRawTransactionResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	txdecoder := assetsMgr.GetTransactionDecoder()
	if txdecoder == nil {
		return util.Error("txdecoder [", req.Symbol, "] is nil")
	}
	rawtx := req.RawTx
	wrapper := open_scanner.NewWrapper(req.AppID, req.WalletID, req.AccountID, req.Symbol)
	if rawtx.Account.IsTrust {
		if err := txdecoder.SignRawTransaction(wrapper, rawtx); err != nil {
			return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]广播交易单签名失败"))
		}
	}
	if err := txdecoder.VerifyRawTransaction(wrapper, rawtx); err != nil {
		return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]广播交易单签名校验失败"))
	}
	if tx, err0 := txdecoder.SubmitRawTransaction(wrapper, rawtx); err0 != nil {
		return webutil.Try(err0, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]广播交易单失败"))
	} else {
		resp.Tx = tx
		resp.TxID = rawtx.TxID
	}
	return nil
}

func (self *WalletApiService) CreateSummaryRawTransaction(req *dto.CreateSummaryRawTransactionReq, resp *dto.CreateSummaryRawTransactionReqResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	txdecoder := assetsMgr.GetTransactionDecoder()
	if txdecoder == nil {
		return util.Error("txdecoder [", req.Symbol, "] is nil")
	}
	wrapper := open_scanner.NewWrapper(req.AppID, req.WalletID, req.AccountID, req.Symbol)
	smrtx := req.Smrtx
	rawtxs, err := txdecoder.CreateSummaryRawTransactionWithError(wrapper, smrtx)
	if err != nil {
		return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]创建汇总交易单失败"))
	}
	for _, v := range rawtxs {
		if v.RawTx == nil {
			v.RawTx = &openwallet.RawTransaction{}
		}
		msg := make(map[string]interface{})
		if v.Error != nil {
			msg["code"] = v.Error.Code()
			msg["err"] = v.Error.Error()
		}
		resp.RawTxs = append(resp.RawTxs, &dto.SmayTx{Tx: v.RawTx, Error: msg})
	}
	return nil
}

func (self *WalletApiService) GetBalanceByAddress(req *dto.GetBalanceByAddressReq, resp *dto.GetBalanceByAddressResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	//提取交易单
	scanner := assetsMgr.GetBlockScanner()
	if scanner == nil {
		return util.Error("[%s] is not block scan", req.Symbol)
	}
	if assetsMgr.BalanceModelType() == openwallet.BalanceModelTypeAddress {
		if req.Address == nil || len(req.Address) == 0 {
			return util.Error("address is nil")
		}
		balances, err := scanner.GetBalanceByAddress(req.Address...)
		if err != nil {
			return util.Error("Can not find balance, unexpected error:", err.Error())
		}
		resp.Balance = balances
	} else if assetsMgr.BalanceModelType() == openwallet.BalanceModelTypeAccount {
		if len(req.AccountID) == 0 {
			return util.Error("address is nil")
		}
		mongo, err := new(sqld.MGOManager).Get()
		if err != nil {
			return util.Error("get mongo error: ", err)
		}
		defer mongo.Close()
		account := model.OwAccount{}
		if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("symbol", req.Symbol).Eq("accountID", req.AccountID), &account); err != nil {
			return err
		}
		if account.Id == 0 {
			return util.Error("account [%s] is nil", req.AccountID)
		}
		balances, err := scanner.GetBalanceByAddress(account.Alias)
		if err != nil {
			return util.Error("Can not find balance, unexpected error:", err.Error())
		}
		resp.Balance = balances
		resp.BalanceType = 1
	}
	return nil
}

func (self *WalletApiService) GetTokenBalanceByAddress(req *dto.GetTokenBalanceByAddressReq, resp *dto.GetTokenBalanceByAddressResp) error {
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	//提取扫块
	decoder := assetsMgr.GetSmartContractDecoder()
	if decoder == nil {
		return util.Error("[%s] is not GetSmartContractDecoder", req.Symbol)
	}
	if assetsMgr.BalanceModelType() == openwallet.BalanceModelTypeAddress {
		if req.Address == nil || len(req.Address) == 0 {
			return util.Error("address is nil")
		}
		balances, err := decoder.GetTokenBalanceByAddress(req.Contract, req.Address...)
		if err != nil {
			return util.Error("Can not find balance, unexpected error:", err.Error())
		}
		resp.Balance = balances
	} else if assetsMgr.BalanceModelType() == openwallet.BalanceModelTypeAccount {
		if len(req.AccountID) == 0 {
			return util.Error("account is nil")
		}
		mongo, err := new(sqld.MGOManager).Get()
		if err != nil {
			return util.Error("get mongo error: ", err)
		}
		defer mongo.Close()
		account := model.OwAccount{}
		if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("symbol", req.Symbol).Eq("accountID", req.AccountID).Eq("state", 1), &account); err != nil {
			return err
		}
		if account.Id == 0 {
			return util.Error("account [%s] is nil", req.AccountID)
		}
		balances, err := decoder.GetTokenBalanceByAddress(req.Contract, account.Alias)
		if err != nil {
			return util.Error("Can not find balance, unexpected error:", err.Error())
		}
		resp.Balance = balances
		resp.BalanceType = 1
	}
	return nil
}

func (self *WalletApiService) GetRawTransactionFeeRate(req *dto.GetRawTransactionFeeRateReq, resp *dto.GetRawTransactionFeeRateResp) error {
	if req.Symbol == "TRX" {
		return nil
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	rate, unit, err := assetsMgr.GetTransactionDecoder().GetRawTransactionFeeRate()
	if err != nil {
		return util.Error("find feerate error: ", err.Error())
	}
	resp.FeeRate = rate
	resp.Unit = unit
	return nil
}

func (self *WalletApiService) RescannerHeight(req *dto.RescannerHeightReq, resp *dto.RescannerHeightResp) error {
	if len(req.Symbol) == 0 {
		return util.Error("symbol [", req.Symbol, "] is nil")
	}
	if req.Height == 0 {
		return util.Error("height [", req.Height, "] is nil")
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	scanner := assetsMgr.GetBlockScanner()

	mlog := assetsMgr.GetAssetsLogger()

	fmt.Println(&mlog)
	scanner.Stop()
	time.Sleep(30 * time.Second)
	err = scanner.SetRescanBlockHeight(uint64(req.Height))
	if err != nil {
		fmt.Println(util.AddStr("设置[", req.Symbol, "][", req.Height, "]重扫高度失败: "), err)
	}
	scanner.Run()
	return nil
}

func (self *WalletApiService) RescannerOneHeight(req *dto.RescannerOneHeightReq, resp *dto.RescannerOneHeightResp) error {
	if len(req.Symbol) == 0 {
		return util.Error("symbol [", req.Symbol, "] is nil")
	}
	if req.Height == 0 {
		return util.Error("height [", req.Height, "] is nil")
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	scanner := assetsMgr.GetBlockScanner()
	scanner.ScanBlock(uint64(req.Height))
	return nil
}

func (self *WalletApiService) GetBalanceType(req *dto.GetBalanceTypeReq, resp *dto.GetBalanceTypeResp) error {
	if len(req.Symbol) == 0 {
		return util.Error("symbol [", req.Symbol, "] is nil")
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	if assetsMgr.BalanceModelType() == openwallet.BalanceModelTypeAddress {
		resp.BalanceType = 0
	} else if assetsMgr.BalanceModelType() == openwallet.BalanceModelTypeAccount {
		resp.BalanceType = 1
	}
	return nil
}

func (self *WalletApiService) OnOffScanner(req *dto.OnOffScannerReq, resp *dto.OnOffScannerResp) error {
	if len(req.Symbol) == 0 {
		return util.Error("symbol [", req.Symbol, "] is nil")
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	if req.OnOff == 1 {
		assetsMgr.GetBlockScanner().Run()
	} else {
		assetsMgr.GetBlockScanner().Stop()
	}
	return nil
}

func (self *WalletApiService) VerifyAddress(req *dto.VerifyAddressReq, resp *dto.VerifyAddressResp) error {
	if len(req.Symbol) == 0 {
		return util.Error("symbol [", req.Symbol, "] is nil")
	}
	if len(req.Address) == 0 {
		return util.Error("address [", req.Address, "] is nil")
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	dec := assetsMgr.GetAddressDecoderV2()
	if dec == nil {
		return util.Error("symbol [", req.Symbol, "] not support")
	}
	resp.Result = dec.AddressVerify(req.Address)
	return nil
}

func getABI(symbol, contractID string) (string, error) {
	if len(symbol) == 0 || len(contractID) == 0 {
		return "", util.Error("symbol or contractID [", contractID, "] is nil")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return "", err
	}
	defer mongo.Close()
	contract := model.OwContract{}
	if err := mongo.FindOne(sqlc.M(model.OwContract{}).Eq("symbol", symbol).Eq("contractID", contractID).Eq("state", 1), &contract); err != nil {
		return "", err
	}
	if contract.Id == 0 {
		return "", util.Error("contractID [", contractID, "] not found")
	}
	return contract.ABI, nil
}

func (self *WalletApiService) CallSmartContractABI(req *dto.CallSmartContractABIReq, resp *dto.CallSmartContractABIResp) error {
	if len(req.Rawtx.Coin.ContractID) > 0 {
		abi, err := getABI(req.Symbol, req.Rawtx.Coin.Contract.ContractID)
		if err != nil {
			return err
		}
		req.Rawtx.Coin.Contract.SetABI(abi)
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	decoder := assetsMgr.GetSmartContractDecoder()
	if decoder == nil {
		return util.Error("[%s] is not GetSmartContractDecoder", req.Symbol)
	}
	wrapper := open_scanner.NewWrapper(req.AppID, req.WalletID, req.AccountID, req.Symbol)
	if ret, err := decoder.CallSmartContractABI(wrapper, req.Rawtx); err != nil {
		return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]调用合约交易单失败: ", err.Error()))
	} else {
		resp.Result = ret
	}
	return nil
}

func (self *WalletApiService) CreateSmartContractTrade(req *dto.CreateSmartContractTradeReq, resp *dto.CreateSmartContractTradeResp) error {
	if len(req.Rawtx.Coin.ContractID) > 0 {
		abi, err := getABI(req.Symbol, req.Rawtx.Coin.Contract.ContractID)
		if err != nil {
			return err
		}
		req.Rawtx.Coin.Contract.SetABI(abi)
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	decoder := assetsMgr.GetSmartContractDecoder()
	if decoder == nil {
		return util.Error("[%s] is not GetSmartContractDecoder", req.Symbol)
	}
	wrapper := open_scanner.NewWrapper(req.AppID, req.WalletID, req.AccountID, req.Symbol)
	tx := req.Rawtx
	if err := decoder.CreateSmartContractRawTransaction(wrapper, tx); err != nil {
		return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]创建合约交易单失败: ", err.Error()))
	}
	resp.Rawtx = tx
	return nil
}

func (self *WalletApiService) SubmitSmartContractTrade(req *dto.SubmitSmartContractTradeReq, resp *dto.SubmitSmartContractTradeResp) error {
	if len(req.Rawtx.Coin.ContractID) > 0 {
		abi, err := getABI(req.Symbol, req.Rawtx.Coin.ContractID)
		if err != nil {
			return err
		}
		req.Rawtx.Coin.Contract.SetABI(abi)
	}
	assetsMgr, err := open_scanner.GetAssetsManager(req.Symbol)
	if err != nil {
		return util.Error("assetsMgr [", req.Symbol, "] is nil")
	}
	decoder := assetsMgr.GetSmartContractDecoder()
	if decoder == nil {
		return util.Error("[%s] is not GetSmartContractDecoder", req.Symbol)
	}
	wrapper := open_scanner.NewWrapper(req.AppID, req.WalletID, req.AccountID, req.Symbol)
	if ret, err := decoder.SubmitSmartContractRawTransaction(wrapper, req.Rawtx); err != nil {
		return webutil.Try(err, util.AddStr("[", req.Symbol, "]账户ID[", req.AccountID, "]广播合约交易单失败: ", err.Error()))
	} else {
		resp.Receipt = ret
	}
	return nil
}
