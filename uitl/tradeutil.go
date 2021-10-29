package tradeutil

import (
	"fmt"
	"github.com/blocktree/go-openw-server/open_base/major"
	"github.com/blocktree/go-openw-server/open_base/model"
	"github.com/blocktree/openwallet/v2/openwallet"
	"github.com/godaddy-x/jorm/amqp"
	"github.com/godaddy-x/jorm/cache/redis"
	"github.com/godaddy-x/jorm/consul"
	"github.com/godaddy-x/jorm/exception"
	"github.com/godaddy-x/jorm/log"
	"github.com/godaddy-x/jorm/sqlc"
	"github.com/godaddy-x/jorm/sqld"
	"github.com/godaddy-x/jorm/util"
	"github.com/nbit99/open_scanner/rpc/dto"
	"github.com/shopspring/decimal"
	"strings"
)

func FmtDiv(v string, d int64) string {
	if len(v) == 0 {
		return "0"
	}
	if d == 0 {
		return v
	}
	x, err := decimal.NewFromString(v)
	if err != nil {
		return "0"
	}
	return x.Shift(-int32(d)).String()
}

func FmtZero(r string) string {
	if len(r) == 0 {
		return "0"
	}
	a := decimal.New(0, 0)
	b, _ := decimal.NewFromString(r)
	a = a.Add(b)
	return a.String()
}

func RebuildFromAddr(trade *model.OwTradeLog, from []string, contract model.OwContract) {
	if from == nil || len(from) == 0 {
		trade.FromAddress = []string{}
		trade.FromAddressV = []string{}
		return
	}
	froms, fromsv := rebuidAddr(from, contract)
	trade.FromAddress = froms
	trade.FromAddressV = fromsv
}

func RebuildToAddr(trade *model.OwTradeLog, to []string, contract model.OwContract) {
	if to == nil || len(to) == 0 {
		trade.ToAddress = []string{}
		trade.ToAddressV = []string{}
		return
	}
	tos, tosv := rebuidAddr(to, contract)
	trade.ToAddress = tos
	trade.ToAddressV = tosv
}

func RebuildTmpFromAddr(trade *model.OwTradeLogTmp, from []string, contract model.OwContract) {
	if from == nil || len(from) == 0 {
		trade.FromAddress = []string{}
		trade.FromAddressV = []string{}
		return
	}
	froms, fromsv := rebuidAddr(from, contract)
	trade.FromAddress = froms
	trade.FromAddressV = fromsv
}

func RebuildTmpToAddr(trade *model.OwTradeLogTmp, to []string, contract model.OwContract) {
	if to == nil || len(to) == 0 {
		trade.ToAddress = []string{}
		trade.ToAddressV = []string{}
		return
	}
	tos, tosv := rebuidAddr(to, contract)
	trade.ToAddress = tos
	trade.ToAddressV = tosv
}

func rebuidAddr(from []string, contract model.OwContract) ([]string, []string) {
	froms := make([]string, 0)
	fromsv := make([]string, 0)
	for e := range from {
		addr := strings.Split(from[e], ":")
		if len(addr) > 1 {
			froms = append(froms, addr[0])
			if contract.Id == 0 {
				fromsv = append(fromsv, addr[1])
			} else {
				// fromsv = append(fromsv, FmtDiv(addr[1], contract.Decimals))
				fromsv = append(fromsv, addr[1])
			}
		} else {
			froms = append(froms, addr[0])
			fromsv = append(fromsv, "")
		}
	}
	return froms, fromsv
}

func GetUserServ(userId string) (*model.OwUserServ, error) {
	if len(userId) == 0 {
		return nil, nil
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, ex.Try{ex.DATA, ex.DATA_ERR, err, nil}
	}
	defer mongo.Close()
	serv := model.OwUserServ{}
	if err := mongo.FindOne(sqlc.M(model.OwUserServ{}).Eq("uid", userId), &serv); err != nil {
		return nil, ex.Try{ex.DATA, ex.DATA_R_ERR, err, nil}
	}
	if serv.Id == 0 {
		return nil, nil
	}
	if serv.ExpireAt < util.Time() {
		return nil, ex.Try{Code: -1, Msg: "服务已到期,请充值续费"}
	}
	return &serv, nil
}

func GetUserServData(appID string) (*model.OwUserServ, *model.OwUserServReport, error) {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, nil, ex.Try{ex.DATA, ex.DATA_ERR, err, nil}
	}
	defer mongo.Close()
	app := model.OwApp{}
	if err := mongo.FindOne(sqlc.M(model.OwApp{}).Eq("appid", appID), &app); err != nil {
		return nil, nil, ex.Try{ex.DATA, ex.DATA_R_ERR, err, nil}
	}
	if app.Id == 0 {
		return nil, nil, ex.Try{Msg: "无效的app数据"}
	}
	serv, err := GetUserServ(app.UserId)
	if err != nil {
		return nil, nil, err
	}
	report, err := GetUserServReport(app.UserId, app.Appid)
	if err != nil {
		return nil, nil, err
	}
	return serv, report, nil
}

func GetUserServReport(userId, appID string) (*model.OwUserServReport, error) {
	serv, err := GetUserServ(userId)
	if err != nil {
		return nil, err
	}
	if serv == nil {
		return nil, nil
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, ex.Try{ex.DATA, ex.DATA_ERR, err, nil}
	}
	defer mongo.Close()
	report := model.OwUserServReport{}
	if err := mongo.FindOne(sqlc.M(model.OwUserServReport{}).Eq("uid", userId).Eq("appID", appID), &report); err != nil {
		return nil, ex.Try{ex.DATA, ex.DATA_R_ERR, err, nil}
	}
	if report.Id == 0 {
		return nil, ex.Try{Msg: "app服务尚未开通"}
	}
	return &report, nil
}

func GetUserServReportSum(userId string) (*model.OwUserServReport, error) {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, ex.Try{ex.DATA, ex.DATA_ERR, err, nil}
	}
	defer mongo.Close()
	report := model.OwUserServReport{}
	if err := mongo.FindOne(sqlc.M(model.OwUserServReport{}).Eq("uid", userId).Agg(sqlc.SUM_, "walletSum").Agg(sqlc.SUM_, "accountSum").Agg(sqlc.SUM_, "addressSum"), &report); err != nil {
		return nil, ex.Try{ex.DATA, ex.DATA_R_ERR, err, nil}
	}
	return &report, nil
}

func FailToReplayBalance(tradelog model.OwTradeLog, contract model.OwContract) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		log.Error("更新余额 - MONGO获取失败", 0, log.String("symbol", tradelog.Symbol), log.AddError(err))
		return nil
	}
	defer mongo.Close()
	try := model.OwTryBalance{}
	if err := mongo.FindOne(sqlc.M(model.OwTryBalance{}).Eq("accountID", tradelog.AccountID).Eq("symbol", tradelog.Symbol).Eq("txid", tradelog.Txid).Eq("blockHeight", tradelog.BlockHeight).Eq("blockHash", tradelog.BlockHash), &try); err != nil {
		log.Error("更新余额 - MONGO查询尝试次数失败", 0, log.String("symbol", tradelog.Symbol), log.AddError(err))
		return nil
	}
	if try.Id == 0 {
		try.AccountID = tradelog.AccountID
		try.Txid = tradelog.Txid
		try.BlockHeight = tradelog.BlockHeight
		try.BlockHash = tradelog.BlockHash
		try.Symbol = tradelog.Symbol
		try.Ctime = util.Time()
		if err := mongo.Save(&try); err != nil {
			log.Error("更新余额 - MONGO保存尝试次数失败", 0, log.String("symbol", tradelog.Symbol), log.AddError(err))
			return nil
		}
	} else if try.Tries > 5 {
		return nil
	}
	try.Tries = try.Tries + 1
	try.Utime = util.Time()
	if err := mongo.Update(&try); err != nil {
		log.Error("更新余额 - MONGO更新尝试次数失败", 0, log.String("symbol", tradelog.Symbol), log.AddError(err))
		return nil
	}
	client, err := new(rabbitmq.PublishManager).Client()
	if err != nil {
		log.Error("获取mq连接失败", 0, log.AddError(err))
		return nil
	}
	content := map[string]interface{}{"tradelog": tradelog, "contract": contract}
	if err := client.Publish(rabbitmq.MsgData{Exchange: "tx.exchange", Queue: "tx.queue.balance", Type: 1, Content: content}); err != nil {
		return nil
	}
	return nil
}

type AddrBalance struct {
	Balance *openwallet.Balance
	Address *model.OwAddress
}

type AddrTokenBalance struct {
	Balance *openwallet.TokenBalance
	Address *model.OwAddress
}

// 局部更新账户余额
func RebuildAccountBalanceFastChange(tradelog model.OwTradeLog) error {

	consulx, err := new(consul.ConsulManager).Client(tradelog.Symbol)
	if err != nil {
		return err
	}
	dreq := &dto.GetBalanceTypeReq{tradelog.Symbol}
	dresp := &dto.GetBalanceTypeResp{}
	if err := consulx.CallService(tradelog.Symbol+"WalletApiService.GetBalanceType", dreq, dresp); err != nil {
		return util.Error("RPC获取余额模型失败: ", err)
	}

	account, err := GetAccount(tradelog.WalletID, tradelog.AccountID)
	if err != nil {
		return err
	}
	if account.Id == 0 {
		return nil
	}

	updateAddress := []interface{}{}
	accountAddressList := map[string]*AddrBalance{}

	for _, v := range tradelog.FromAddress {
		if dresp.BalanceType == 0 { // 余额模型
			if err := updateAccountBalanceByGetAddress(accountAddressList, tradelog, v); err != nil {
				return err
			}
		} else { // 账户模型
			if err := updateAccountBalanceByT1(account, tradelog, v); err != nil {
				return err
			}
		}
	}
	for _, v := range tradelog.ToAddress {
		if dresp.BalanceType == 0 {
			if err := updateAccountBalanceByGetAddress(accountAddressList, tradelog, v); err != nil {
				return err
			}
		} else {
			if err := updateAccountBalanceByT1(account, tradelog, v); err != nil {
				return err
			}
		}
	}

	if len(accountAddressList) > 0 {
		updateAccountBalance(account, &updateAddress, accountAddressList)
	}

	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()

	if err := mongo.UpdateByCnd(sqlc.M(model.OwAccount{}).Eq("id", account.Id).UpdateKeyValue([]string{"balance", "confirmBalance", "unconfirmBalance", "utime"}, account.Balance, account.ConfirmBalance, account.UnconfirmBalance, util.Time())); err != nil {
		return util.Error("局部更新账户余额失败: ", err)
	}
	if len(updateAddress) > 0 {
		for _, v := range updateAddress {
			address := v.(*model.OwAddress)
			if err := mongo.UpdateByCnd(sqlc.M(model.OwAddress{}).Eq("id", address.Id).UpdateKeyValue([]string{"balance", "confirmBalance", "unconfirmBalance", "utime"}, address.Balance, address.ConfirmBalance, address.UnconfirmBalance, util.Time())); err != nil {
				log.Error(util.AddStr("局部更新地址余额失败"), 0, log.Any("data", v), log.AddError(err))
			}
		}
	}
	return nil
}

// 局部更新地址模型余额
func updateAccountBalance(account *model.OwAccount, updateAddress *[]interface{}, addressBalance map[string]*AddrBalance) error {

	changeBalance := decimal.New(0, 0)
	changeConfirmBalance := decimal.New(0, 0)
	changeUnconfirmBalance := decimal.New(0, 0)

	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()

	for _, v := range addressBalance {
		address := &model.OwAddress{}
		if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Fields("id", "balance", "confirmBalance", "unconfirmBalance").Eq("id", v.Address.Id), address); err != nil {
			return err
		}
		if address.Id == 0 {
			continue
		}
		b := v.Balance
		// 新的地址余额 - 旧的地址余额 = 差值
		oldAddressBalance := decimal.New(0, 0)
		if len(address.Balance) > 0 {
			oldAddressBalance, _ = decimal.NewFromString(address.Balance)
		}
		oldAddressConfirmBalance := decimal.New(0, 0)
		if len(address.ConfirmBalance) > 0 {
			oldAddressConfirmBalance, _ = decimal.NewFromString(address.ConfirmBalance)
		}
		oldAddressUnconfirmBalance := decimal.New(0, 0)
		if len(address.UnconfirmBalance) > 0 {
			oldAddressUnconfirmBalance, _ = decimal.NewFromString(address.UnconfirmBalance)
		}

		newAddressBalance, _ := decimal.NewFromString(b.Balance)
		newAddressConfirmBalance, _ := decimal.NewFromString(b.ConfirmBalance)
		newAddressUnconfirmBalance, _ := decimal.NewFromString(b.UnconfirmBalance)

		changeBalance = changeBalance.Add(newAddressBalance.Sub(oldAddressBalance))
		changeConfirmBalance = changeConfirmBalance.Add(newAddressConfirmBalance.Sub(oldAddressConfirmBalance))
		changeUnconfirmBalance = changeUnconfirmBalance.Add(newAddressUnconfirmBalance.Sub(oldAddressUnconfirmBalance))

		address.Balance = newAddressBalance.String()
		address.ConfirmBalance = newAddressConfirmBalance.String()
		address.UnconfirmBalance = newAddressUnconfirmBalance.String()

		address.Utime = util.Time()
		*updateAddress = append(*updateAddress, address)
	}

	if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Fields("id", "balance", "confirmBalance", "unconfirmBalance").Eq("id", account.Id), account); err != nil {
		return err
	}

	if account.Id == 0 {
		return nil
	}

	// 账号余额 + 差值 = 最新总账号余额
	accountBalance := decimal.New(0, 0)
	if len(account.Balance) > 0 {
		accountBalance, _ = decimal.NewFromString(account.Balance)
	}
	accountConfirmBalance := decimal.New(0, 0)
	if len(account.ConfirmBalance) > 0 {
		accountConfirmBalance, _ = decimal.NewFromString(account.ConfirmBalance)
	}
	accountUnconfirmBalance := decimal.New(0, 0)
	if len(account.UnconfirmBalance) > 0 {
		accountUnconfirmBalance, _ = decimal.NewFromString(account.UnconfirmBalance)
	}

	account.Balance = accountBalance.Add(changeBalance).String()
	account.ConfirmBalance = accountConfirmBalance.Add(changeConfirmBalance).String()
	account.UnconfirmBalance = accountUnconfirmBalance.Add(changeUnconfirmBalance).String()

	account.Utime = util.Time()

	return nil
}

// 局部更新地址模型余额 - 获取变动地址余额
func updateAccountBalanceByGetAddress(addressBalance map[string]*AddrBalance, tradelog model.OwTradeLog, addrv string) error {
	if _, ok := addressBalance[addrv]; ok {
		return nil
	}

	address, err := GetAddress(tradelog.AccountID, addrv)
	if err != nil {
		return err
	}
	if address.Id == 0 {
		return nil
	}

	symbol := tradelog.Symbol
	consulx, err := new(consul.ConsulManager).Client(symbol)
	if err != nil {
		return err
	}
	dreq := &dto.GetBalanceByAddressReq{symbol, []string{addrv}, address.AccountID}
	dresp := &dto.GetBalanceByAddressResp{}
	if err := consulx.CallService(symbol+"WalletApiService.GetBalanceByAddress", dreq, dresp); err != nil {
		return util.Error("RPC获取地址余额列表失败: ", err)
	}

	for _, b := range dresp.Balance {
		addressBalance[address.Address] = &AddrBalance{Balance: b, Address: address}
	}
	return nil
}

// 局部更新账户模型余额
func updateAccountBalanceByT1(account *model.OwAccount, tradelog model.OwTradeLog, addrv string) error {
	symbol := tradelog.Symbol
	consulx, err := new(consul.ConsulManager).Client(symbol)
	if err != nil {
		return err
	}
	dreq := &dto.GetBalanceByAddressReq{symbol, []string{addrv}, account.AccountID}
	dresp := &dto.GetBalanceByAddressResp{}
	if err := consulx.CallService(symbol+"WalletApiService.GetBalanceByAddress", dreq, dresp); err != nil {
		return util.Error("RPC获取地址余额列表失败: ", err)
	}
	if dresp.Balance == nil || len(dresp.Balance) == 0 {
		return nil
	}
	b := dresp.Balance[0]
	account.Balance = b.Balance
	account.ConfirmBalance = b.ConfirmBalance
	account.UnconfirmBalance = b.UnconfirmBalance
	account.Utime = util.Time()
	return nil
}

// 局部更新账户token余额
func RebuildAccountTokenBalanceFastChange(tradelog model.OwTradeLog, contract model.OwContract) error {
	if contract.Id == 0 {
		return nil
	}
	consulx, err := new(consul.ConsulManager).Client(tradelog.Symbol)
	if err != nil {
		return err
	}
	dreq := &dto.GetBalanceTypeReq{tradelog.Symbol}
	dresp := &dto.GetBalanceTypeResp{}
	if err := consulx.CallService(tradelog.Symbol+"WalletApiService.GetBalanceType", dreq, dresp); err != nil {
		return util.Error("RPC获取账户余额模型失败: ", err)
	}
	account, err := GetAccount(tradelog.WalletID, tradelog.AccountID)
	if err != nil {
		return err
	}
	if account.Id == 0 {
		//if tradelog.Symbol == model.ETH { // 如是ETH链则二次判定是否存在TRUE链公用地址
		//	if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("accountID", tradelog.AccountID).Eq("symbol", model.TRUE).Eq("state", 1), &account); err != nil {
		//		return err
		//	}
		//}
		//if account.Id == 0 {
		//	return nil
		//}
		return nil
	}

	accountToken, err := GetAccountToken(account.WalletID, account.AccountID, contract.ContractID)
	if err != nil {
		return err
	}
	if accountToken.Id == 0 {
		accountToken.AppID = tradelog.AppID
		accountToken.WalletID = account.WalletID
		accountToken.AccountID = account.AccountID
		accountToken.ContractID = contract.ContractID
		accountToken.Symbol = contract.Symbol
		accountToken.Token = contract.Token
		accountToken.Balance = "0"
		accountToken.ConfirmBalance = "0"
		accountToken.UnconfirmBalance = "0"
		accountToken.Ctime = util.Time()
		accountToken.Utime = util.Time()
		accountToken.State = 1
	}

	saveAddressToken := []interface{}{}
	updateAddressToken := []interface{}{}

	accountAddressList := map[string]*AddrTokenBalance{}

	for _, v := range tradelog.FromAddress {
		if dresp.BalanceType == 0 {
			if err := updateAccountTokenBalanceByGetAddress(accountAddressList, account, tradelog, contract, v); err != nil {
				return err
			}
		} else {
			if err := updateAddressTokenBalanceByT1(account, accountToken, tradelog, contract, v); err != nil {
				return err
			}
		}
	}
	for _, v := range tradelog.ToAddress {
		if dresp.BalanceType == 0 {
			if err := updateAccountTokenBalanceByGetAddress(accountAddressList, account, tradelog, contract, v); err != nil {
				return err
			}
		} else {
			if err := updateAddressTokenBalanceByT1(account, accountToken, tradelog, contract, v); err != nil {
				return err
			}
		}
	}
	if len(accountAddressList) > 0 {
		updateAddressTokenBalance(accountToken, &saveAddressToken, &updateAddressToken, accountAddressList, tradelog, contract)
	}

	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()

	if accountToken.Id == 0 {
		if err := mongo.Save(accountToken); err != nil {
			return util.Error("保存账户token失败: ", err)
		}
	} else {
		if err := mongo.UpdateByCnd(sqlc.M(model.OwAccountToken{}).Eq("id", accountToken.Id).UpdateKeyValue([]string{"balance", "confirmBalance", "unconfirmBalance", "utime"}, accountToken.Balance, accountToken.ConfirmBalance, accountToken.UnconfirmBalance, util.Time())); err != nil {
			return util.Error("更新账户token失败: ", err)
		}
	}
	if len(saveAddressToken) > 0 {
		if err := mongo.Save(saveAddressToken...); err != nil {
			return util.Error("批量保存地址token失败: ", err)
		}
	}
	if len(updateAddressToken) > 0 {
		for _, v := range updateAddressToken {
			addressToken := v.(*model.OwAddressToken)
			if err := mongo.UpdateByCnd(sqlc.M(model.OwAddressToken{}).Eq("id", addressToken.Id).UpdateKeyValue([]string{"balance", "confirmBalance", "unconfirmBalance", "utime"}, addressToken.Balance, addressToken.ConfirmBalance, addressToken.UnconfirmBalance, util.Time())); err != nil {
				log.Error("更新地址token失败", 0, log.AddError(err))
			}
		}
	}
	return nil
}

// 局部更新地址模型token余额 - 获取变动地址余额
func updateAccountTokenBalanceByGetAddress(addressBalance map[string]*AddrTokenBalance, account *model.OwAccount, tradelog model.OwTradeLog, contract model.OwContract, addrv string) error {
	if _, ok := addressBalance[addrv]; ok {
		return nil
	}
	address, err := GetAddress(account.AccountID, addrv)
	if err != nil {
		return err
	}
	if address.Id == 0 {
		return nil
	}
	consulx, err := new(consul.ConsulManager).Client(address.Symbol)
	if err != nil {
		return err
	}
	smartContract := contract.ToSmartContract()
	smartContract.Decimals = 0
	addressStr := []string{address.Address}
	dreq := &dto.GetTokenBalanceByAddressReq{tradelog.Symbol, smartContract, addressStr, address.AccountID}
	dresp := &dto.GetTokenBalanceByAddressResp{}
	if err := consulx.CallService(tradelog.Symbol+"WalletApiService.GetTokenBalanceByAddress", dreq, dresp); err != nil {
		return util.Error("RPC获取地址列表余额失败: ", err)
	}
	for _, b := range dresp.Balance {
		addressBalance[address.Address] = &AddrTokenBalance{Balance: b, Address: address}
	}
	return nil
}

// 局部更新地址模型token余额
func updateAddressTokenBalance(accountToken *model.OwAccountToken, saveAddressToken *[]interface{}, updateAddressToken *[]interface{}, addressBalance map[string]*AddrTokenBalance, tradelog model.OwTradeLog, contract model.OwContract) error {

	changeBalance := decimal.New(0, 0)
	changeConfirmBalance := decimal.New(0, 0)
	changeUnconfirmBalance := decimal.New(0, 0)

	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()

	for _, v := range addressBalance {

		b := v.Balance
		address := v.Address

		newAddressBalance, _ := decimal.NewFromString(b.Balance.Balance)
		newAddressConfirmBalance, _ := decimal.NewFromString(b.Balance.ConfirmBalance)
		newAddressUnconfirmBalance, _ := decimal.NewFromString(b.Balance.UnconfirmBalance)

		// 保存用户address token余额数据,如不存在则新增
		addressToken, err := GetAddressToken(address.WalletID, address.AccountID, b.Balance.Address, contract.ContractID)
		if err != nil {
			return err
		}
		if addressToken.Id == 0 {

			changeBalance = changeBalance.Add(newAddressBalance)
			changeConfirmBalance = changeConfirmBalance.Add(newAddressConfirmBalance)
			changeUnconfirmBalance = changeUnconfirmBalance.Add(newAddressUnconfirmBalance)

			addressToken.AppID = tradelog.AppID
			addressToken.WalletID = address.WalletID
			addressToken.AccountID = address.AccountID
			addressToken.Address = b.Balance.Address
			addressToken.ContractID = contract.ContractID
			addressToken.Symbol = contract.Symbol
			addressToken.Token = contract.Token

			addressToken.Balance = newAddressBalance.String()
			addressToken.ConfirmBalance = newAddressConfirmBalance.String()
			addressToken.UnconfirmBalance = newAddressUnconfirmBalance.String()

			addressToken.Ctime = util.Time()
			addressToken.Utime = util.Time()
			addressToken.State = 1

			*saveAddressToken = append(*saveAddressToken, addressToken)
		} else {

			if err := mongo.FindOne(sqlc.M(model.OwAddressToken{}).Fields("id", "balance", "confirmBalance", "unconfirmBalance").Eq("id", addressToken.Id), addressToken); err != nil {
				return err
			}
			if addressToken.Id == 0 {
				continue
			}

			// 新的地址余额 - 旧的地址余额 = 差值
			oldAddressBalance := decimal.New(0, 0)
			if len(addressToken.Balance) > 0 {
				oldAddressBalance, _ = decimal.NewFromString(addressToken.Balance)
			}
			oldAddressConfirmBalance := decimal.New(0, 0)
			if len(addressToken.ConfirmBalance) > 0 {
				oldAddressConfirmBalance, _ = decimal.NewFromString(addressToken.ConfirmBalance)
			}
			oldAddressUnconfirmBalance := decimal.New(0, 0)
			if len(addressToken.UnconfirmBalance) > 0 {
				oldAddressUnconfirmBalance, _ = decimal.NewFromString(addressToken.UnconfirmBalance)
			}

			changeBalance = changeBalance.Add(newAddressBalance.Sub(oldAddressBalance))
			changeConfirmBalance = changeConfirmBalance.Add(newAddressConfirmBalance.Sub(oldAddressConfirmBalance))
			changeUnconfirmBalance = changeUnconfirmBalance.Add(newAddressUnconfirmBalance.Sub(oldAddressUnconfirmBalance))

			addressToken.Balance = newAddressBalance.String()
			addressToken.ConfirmBalance = newAddressConfirmBalance.String()
			addressToken.UnconfirmBalance = newAddressUnconfirmBalance.String()

			addressToken.Utime = util.Time()

			*updateAddressToken = append(*updateAddressToken, addressToken)
		}
	}

	if accountToken.Id > 0 {
		if err := mongo.FindOne(sqlc.M(model.OwAccountToken{}).Fields("id", "balance", "confirmBalance", "unconfirmBalance").Eq("id", accountToken.Id), accountToken); err != nil {
			return err
		}
		if accountToken.Id == 0 {
			return nil
		}
	}

	accountBalance := decimal.New(0, 0)
	if len(accountToken.Balance) > 0 {
		accountBalance, _ = decimal.NewFromString(accountToken.Balance)
	}
	accountConfirmBalance := decimal.New(0, 0)
	if len(accountToken.ConfirmBalance) > 0 {
		accountConfirmBalance, _ = decimal.NewFromString(accountToken.ConfirmBalance)
	}
	accountUnconfirmBalance := decimal.New(0, 0)
	if len(accountToken.UnconfirmBalance) > 0 {
		accountUnconfirmBalance, _ = decimal.NewFromString(accountToken.UnconfirmBalance)
	}

	accountBalance = accountBalance.Add(changeBalance)
	accountConfirmBalance = accountConfirmBalance.Add(changeConfirmBalance)
	accountUnconfirmBalance = accountUnconfirmBalance.Add(changeUnconfirmBalance)

	accountToken.Balance = accountBalance.String()
	accountToken.ConfirmBalance = accountConfirmBalance.String()
	accountToken.UnconfirmBalance = accountUnconfirmBalance.String()

	accountToken.Utime = util.Time()
	return nil
}

// 局部更新账户模型token余额
func updateAddressTokenBalanceByT1(account *model.OwAccount, accountToken *model.OwAccountToken, tradelog model.OwTradeLog, contract model.OwContract, addrv string) error {
	symbol := tradelog.Symbol
	consulx, err := new(consul.ConsulManager).Client(account.Symbol)
	if err != nil {
		return err
	}
	smartContract := contract.ToSmartContract()
	smartContract.Decimals = 0
	dreq := &dto.GetTokenBalanceByAddressReq{symbol, smartContract, []string{}, account.AccountID}
	dresp := &dto.GetTokenBalanceByAddressResp{}
	if err := consulx.CallService(symbol+"WalletApiService.GetTokenBalanceByAddress", dreq, dresp); err != nil {
		return util.Error("RPC获取地址列表余额失败: ", err)
	}
	if dresp.Balance == nil || len(dresp.Balance) == 0 {
		return nil
	}
	b := dresp.Balance[0].Balance
	accountToken.Balance = b.Balance
	accountToken.ConfirmBalance = b.ConfirmBalance
	accountToken.UnconfirmBalance = b.UnconfirmBalance
	accountToken.Utime = util.Time()
	return nil
}

// 全量更新账户余额
func RebuildAccountBalance(appID, accountID, symbol string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		log.Error("更新余额 - MGO获取失败", 0, log.String("symbol", symbol), log.AddError(err))
		return nil
	}
	defer mongo.Close()
	account := model.OwAccount{}
	if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("appID", appID).Eq("accountID", accountID).Eq("symbol", symbol).Eq("state", 1), &account); err != nil {
		log.Error("获取更新余额账号失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
		return nil
	}
	if account.Id == 0 {
		log.Error("获取更新余额账号不存在", 0, log.String("symbol", symbol), log.String("accountID", accountID))
		return nil
	}
	consulx, err := new(consul.ConsulManager).Client(symbol)
	if err != nil {
		log.Error("获取RPC失败 - WalletApiService.GetBalanceByAddress", 0, log.String("symbol", symbol))
		return nil
	}
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	query := sqlc.M(model.OwAddress{}).Eq("appID", account.AppID).Eq("accountID", account.AccountID).Eq("symbol", symbol).Eq("state", 1).Limit(1, 100)
	if _, err := mongo.Count(query); err != nil {
		log.Error("获取账号地址列表统计数失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
		return nil
	}
	pagin := query.Pagination
	// account总余额
	accountBalance := decimal.New(0, 0)
	accountConfirmBalance := decimal.New(0, 0)
	accountUnconfirmBalance := decimal.New(0, 0)
	for i := int64(1); i <= pagin.PageCount; i++ {
		addressList := []*model.OwAddress{}
		if err := mongo.FindList(query.Limit(i, pagin.PageSize), &addressList); err != nil {
			log.Error("获取账号地址列表失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
			continue
		}
		addressStr := make([]string, 0)
		for _, a := range addressList {
			addressStr = append(addressStr, a.Address)
			if err := major.CheckAndSaveCacheV(client, util.AddStr(a.Symbol, a.Address), &major.CacheValue{V: a.AccountID}); err != nil {
				return util.Error("查询或保存地址[", a.Address, "]缓存失败: ", err)
			}
		}
		dreq := &dto.GetBalanceByAddressReq{symbol, addressStr, accountID}
		dresp := &dto.GetBalanceByAddressResp{}
		if err := consulx.CallService(symbol+"WalletApiService.GetBalanceByAddress", dreq, dresp); err != nil {
			log.Error("RPC获取地址列表余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.Any("request", dreq), log.AddError(err))
			continue
		}
		if dresp.BalanceType == 1 && dresp.Balance != nil && len(dresp.Balance) > 0 {
			b := dresp.Balance[0]
			account.Balance = b.Balance
			account.ConfirmBalance = b.ConfirmBalance
			account.UnconfirmBalance = b.UnconfirmBalance
			account.Utime = util.Time()
			if err := mongo.Update(&account); err != nil {
				log.Error("更新账户余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.AddError(err))
			}
			return nil
		}
		for _, b := range dresp.Balance {

			addressBalance, _ := decimal.NewFromString(b.Balance)
			addressConfirmBalance, _ := decimal.NewFromString(b.ConfirmBalance)
			addressUnconfirmBalance, _ := decimal.NewFromString(b.UnconfirmBalance)

			accountBalance = accountBalance.Add(addressBalance)
			accountConfirmBalance = accountConfirmBalance.Add(addressConfirmBalance)
			accountUnconfirmBalance = accountUnconfirmBalance.Add(addressUnconfirmBalance)

			address := model.OwAddress{}
			if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("accountID", account.AccountID).Eq("address", b.Address).Eq("state", 1), &address); err != nil {
				log.Error("查询地址余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.String("address", b.Address), log.AddError(err))
				continue
			}
			if address.Id > 0 {
				address.Balance = addressBalance.String()
				address.ConfirmBalance = addressConfirmBalance.String()
				address.UnconfirmBalance = addressUnconfirmBalance.String()
				address.Utime = util.Time()
				// log.Info("全量更新账号余额 - address", 0, log.String("symbol", account.Symbol), log.String("accountID", account.AccountID), log.Int64("index", address.AddrIndex), log.String("address", address.Address), log.String("balance", address.Balance))
				if err := mongo.UpdateByCnd(sqlc.M(model.OwAddress{}).Eq("id", address.Id).UpdateKeyValue([]string{"balance", "confirmBalance", "unconfirmBalance", "utime"}, address.Balance, address.ConfirmBalance, address.UnconfirmBalance, util.Time())); err != nil {
					log.Error("更新账户余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.AddError(err))
				}
				fmt.Println("start account address ----- ", i, address.AddrIndex, address.Symbol, address.AccountID, address.Address, address.Balance, address.ConfirmBalance, address.UnconfirmBalance)
			}
		}
	}

	account.Balance = accountBalance.String()
	account.ConfirmBalance = accountConfirmBalance.String()
	account.UnconfirmBalance = accountUnconfirmBalance.String()

	account.Utime = util.Time()

	// log.Info("全量更新账号余额 - account", 0, log.String("symbol", account.Symbol), log.String("accountID", account.AccountID), log.String("balance", account.Balance))

	//if err := mongo.Update(&account); err != nil {
	//	log.Error("更新账户余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.AddError(err))
	//}

	if err := mongo.UpdateByCnd(sqlc.M(model.OwAccount{}).Eq("id", account.Id).UpdateKeyValue([]string{"balance", "confirmBalance", "unconfirmBalance", "utime"}, account.Balance, account.ConfirmBalance, account.UnconfirmBalance, util.Time())); err != nil {
		log.Error("更新账户余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.AddError(err))
	}

	//if err := mongo.Update(updateAddrs...); err != nil {
	//	log.Error("更新地址余额失败", 0, log.String("symbol", symbol), log.String("accountID", account.AccountID), log.Any("address", updateAddrs), log.AddError(err))
	//}

	if len(account.Alias) > 0 {
		if err := major.CheckAndSaveCacheV(client, util.AddStr(account.Symbol, account.Alias), &major.CacheValue{V: account.AccountID}); err != nil {
			return util.Error("查询或保存账号[", account.Symbol, "][", account.Alias, "]缓存失败: ", err)
		}
	}
	return nil
}

// 全量更新账户token余额
func RebuildAccountToken(appID, walletID, accountID string, contract model.OwContract) error {
	fmt.Println("-----------------------", accountID, contract.Token)
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		log.Error("更新余额 - MGO获取失败", 0, log.AddError(err))
		return nil
	}
	defer mongo.Close()
	symbol := contract.Symbol
	consulx, err := new(consul.ConsulManager).Client(symbol)
	if err != nil {
		log.Error("获取RPC失败 - WalletApiService.GetTokenBalanceByAddress", 0, log.String("symbol", symbol))
		return nil
	}
	query := sqlc.M(model.OwAddress{}).Eq("appID", appID).Eq("accountID", accountID).Eq("state", 1).Limit(1, 100)
	if _, err := mongo.Count(query); err != nil {
		log.Error("获取账号地址列表统计数失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
		return nil
	}
	fmt.Println("rebuild balance address total: ", query.Pagination.PageTotal)
	//account总余额
	accountBalance := decimal.New(0, 0)
	accountConfirmBalance := decimal.New(0, 0)
	accountUnconfirmBalance := decimal.New(0, 0)

	saveTokens := []interface{}{}
	updateTokens := []interface{}{}

	pagin := query.Pagination
	for i := int64(1); i <= pagin.PageCount; i++ {
		// 合约交易,更新token余额
		addressList := []*model.OwAddress{}
		if err := mongo.FindList(query.Limit(i, pagin.PageSize), &addressList); err != nil {
			log.Error("获取账号地址列表失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
			continue
		}
		addressStr := make([]string, 0)
		for _, a := range addressList {
			addressStr = append(addressStr, a.Address)
		}
		smartContract := contract.ToSmartContract()
		smartContract.Decimals = 0
		dreq := &dto.GetTokenBalanceByAddressReq{symbol, smartContract, addressStr, accountID}
		dresp := &dto.GetTokenBalanceByAddressResp{}
		if err := consulx.CallService(symbol+"WalletApiService.GetTokenBalanceByAddress", dreq, dresp); err != nil {
			log.Error("RPC获取地址列表余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.Any("request", dreq), log.AddError(err))
			continue
		}

		fmt.Println("rebuild balance token address total: ", len(dresp.Balance))

		if dresp.BalanceType == 1 && dresp.Balance != nil && len(dresp.Balance) > 0 {
			b := dresp.Balance[0].Balance

			// fmt.Println("start address token address ----- ", i, accountID, b.Address, b.Balance, b.ConfirmBalance, b.UnconfirmBalance)

			// 保存用户token余额数据,如不存在则新增
			accountToken := model.OwAccountToken{}
			if err := mongo.FindOne(sqlc.M(model.OwAccountToken{}).Eq("appID", appID).Eq("walletID", walletID).Eq("accountID", accountID).Eq("contractID", contract.ContractID).Eq("state", 1), &accountToken); err != nil {
				log.Error("获取账号token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
				continue
			}
			if accountToken.Id == 0 {
				accountToken.AppID = appID
				accountToken.WalletID = walletID
				accountToken.AccountID = accountID
				accountToken.ContractID = contract.ContractID
				accountToken.Symbol = contract.Symbol
				accountToken.Token = contract.Token
				accountToken.Balance = b.Balance
				accountToken.ConfirmBalance = b.ConfirmBalance
				accountToken.UnconfirmBalance = b.UnconfirmBalance
				accountToken.Ctime = util.Time()
				accountToken.Utime = util.Time()
				accountToken.State = 1
				if err := mongo.Save(&accountToken); err != nil {
					log.Error("更新账号token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.AddError(err))
				}
			} else {
				accountToken.Balance = b.Balance
				accountToken.ConfirmBalance = b.ConfirmBalance
				accountToken.UnconfirmBalance = b.UnconfirmBalance
				accountToken.Utime = util.Time()
				if err := mongo.Update(&accountToken); err != nil {
					log.Error("更新账号token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.AddError(err))
				}
			}
			return nil
		}
		for _, s := range dresp.Balance {
			if s.Contract.ContractID == contract.ContractID {

				addressBalance, err := decimal.NewFromString(s.Balance.Balance)
				addressConfirmBalance, err := decimal.NewFromString(s.Balance.ConfirmBalance)
				addressUnconfirmBalance, err := decimal.NewFromString(s.Balance.UnconfirmBalance)

				if err != nil {
					log.Error("token余额计算失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.String("address", s.Balance.Address), log.Any("contract", contract), log.AddError(err))
					continue
				}

				accountBalance = accountBalance.Add(addressBalance)
				accountConfirmBalance = accountConfirmBalance.Add(addressConfirmBalance)
				accountUnconfirmBalance = accountUnconfirmBalance.Add(addressUnconfirmBalance)

				// fmt.Println("start address token address ----- ", i, accountID, s.Balance.Address, s.Balance.Balance, s.Balance.ConfirmBalance, s.Balance.UnconfirmBalance)

				// 保存用户address token余额数据,如不存在则新增
				addressToken := model.OwAddressToken{}
				if err := mongo.FindOne(sqlc.M(model.OwAddressToken{}).Eq("appID", appID).Eq("accountID", accountID).Eq("address", s.Balance.Address).Eq("contractID", contract.ContractID).Eq("state", 1), &addressToken); err != nil {
					log.Error("获取账号地址token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.String("address", s.Balance.Address))
					continue
				}

				if addressToken.Id == 0 {
					addressToken.AppID = appID
					addressToken.WalletID = walletID
					addressToken.AccountID = accountID
					addressToken.Address = s.Balance.Address
					addressToken.ContractID = contract.ContractID
					addressToken.Symbol = contract.Symbol
					addressToken.Token = contract.Token

					addressToken.Balance = addressBalance.String()
					addressToken.ConfirmBalance = addressConfirmBalance.String()
					addressToken.UnconfirmBalance = addressUnconfirmBalance.String()

					addressToken.Ctime = util.Time()
					addressToken.Utime = util.Time()
					addressToken.State = 1

					// log.Info("全量更新账号token余额 - address", 0, log.String("symbol", addressToken.Symbol), log.String("contractID", addressToken.ContractID), log.Any("contract", contract), log.String("accountID", addressToken.AccountID), log.String("address", addressToken.Address), log.String("balance", addressToken.Balance))
					saveTokens = append(saveTokens, &addressToken)
				} else {

					addressToken.Balance = addressBalance.String()
					addressToken.ConfirmBalance = addressConfirmBalance.String()
					addressToken.UnconfirmBalance = addressUnconfirmBalance.String()

					addressToken.Utime = util.Time()
					// log.Info("全量更新账号token余额 - address", 0, log.String("symbol", addressToken.Symbol), log.String("contractID", addressToken.ContractID), log.Any("contract", contract), log.String("accountID", addressToken.AccountID), log.String("address", addressToken.Address), log.String("balance", addressToken.Balance))
					updateTokens = append(updateTokens, &addressToken)
				}
			}
		}
	}
	// 保存用户token余额数据,如不存在则新增
	accountToken := model.OwAccountToken{}
	if err := mongo.FindOne(sqlc.M(model.OwAccountToken{}).Eq("appID", appID).Eq("walletID", walletID).Eq("accountID", accountID).Eq("contractID", contract.ContractID).Eq("state", 1), &accountToken); err != nil {
		log.Error("获取账号token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID))
	}
	if accountToken.Id == 0 {
		accountToken.AppID = appID
		accountToken.WalletID = walletID
		accountToken.AccountID = accountID
		accountToken.ContractID = contract.ContractID
		accountToken.Symbol = contract.Symbol
		accountToken.Token = contract.Token

		accountToken.Balance = accountBalance.String()
		accountToken.ConfirmBalance = accountConfirmBalance.String()
		accountToken.UnconfirmBalance = accountUnconfirmBalance.String()

		// log.Info("全量更新账号token余额 - account", 0, log.String("symbol", accountToken.Symbol), log.String("accountID", accountToken.AccountID), log.String("balance", accountToken.Balance))

		accountToken.Ctime = util.Time()
		accountToken.Utime = util.Time()
		accountToken.State = 1
		if err := mongo.Save(&accountToken); err != nil {
			log.Error("更新账号token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.AddError(err))
		}
	} else {

		accountToken.Balance = accountBalance.String()
		accountToken.ConfirmBalance = accountConfirmBalance.String()
		accountToken.UnconfirmBalance = accountUnconfirmBalance.String()

		// log.Info("全量更新账号token余额 - account", 0, log.String("symbol", accountToken.Symbol), log.String("accountID", accountToken.AccountID), log.String("balance", accountToken.Balance))

		accountToken.Utime = util.Time()
		if err := mongo.Update(&accountToken); err != nil {
			log.Error("更新账号token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.AddError(err))
		}
	}
	if len(saveTokens) > 0 {
		for _, v := range saveTokens {
			if err := mongo.Save(v); err != nil {
				log.Error("新增地址token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.Any("data", saveTokens[0]), log.AddError(err))
			}
		}
	}
	if len(updateTokens) > 0 {
		for _, v := range updateTokens {
			if err := mongo.Update(v); err != nil {
				log.Error("更新地址token余额失败", 0, log.String("symbol", symbol), log.String("accountID", accountID), log.Any("data", updateTokens[0]), log.AddError(err))
			}
		}
	}
	return nil
}

func RebuildFreerate(symbol string) error {
	if symbol == "TRX" {
		return nil
	}
	consulx, err := new(consul.ConsulManager).Client(symbol)
	if err != nil {
		return err
	}
	dreq := &dto.GetRawTransactionFeeRateReq{symbol}
	dresp := &dto.GetRawTransactionFeeRateResp{}
	if err := consulx.CallService(symbol+"WalletApiService.GetRawTransactionFeeRate", dreq, dresp); err != nil {
		return err
	}
	redis, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	result := map[string]interface{}{
		"feeRate": dresp.FeeRate,
		"unit":    dresp.Unit,
		"symbol":  symbol,
	}
	if err := redis.Put(util.AddStr("fr_", symbol), &result); err != nil {
		return err
	}
	return nil
}

func RebuildMaxblock(curBlock *model.OwBlock) error {
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	if err := client.Put(util.AddStr("tx.block.coin.", curBlock.Symbol), curBlock); err != nil {
		return util.Error(util.AddStr("写入[", curBlock.Symbol, "]最新区块缓存失败: ", err.Error()))
	}
	log.Debug("最新区块高度缓存成功", 0, log.Int64("blockId", curBlock.Id), log.String("symbol", curBlock.Symbol), log.Int64("height", curBlock.Height))
	return nil
}

func RebuildDxLisblock(curBlock *model.OwBlock) error {
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	if err := client.Del("dx.block.coin." + curBlock.Symbol); err != nil {
		log.Warn("删除最新监听区块["+curBlock.Symbol+"]失败", 0)
	}
	return nil
}
