package open_scanner

import (
	"errors"
	"fmt"
	"github.com/godaddy-x/jorm/sqlc"
	"github.com/godaddy-x/jorm/sqld"
	"github.com/godaddy-x/jorm/util"
	"github.com/nbit99/open_base/model"
	"github.com/nbit99/openwallet/v2/common"
	"github.com/nbit99/openwallet/v2/hdkeystore"
	"github.com/nbit99/openwallet/v2/log"
	"github.com/nbit99/openwallet/v2/openwallet"
	"strings"
	"time"
)

const (
	FilterBalanceKey = "FilterBalance"
)

type RpcWrapper struct {
	*openwallet.WalletDAIBase
	AppID     string
	WalletID  string
	AccountID string
	Symbol    string
	key       *hdkeystore.HDKey
}

func (w *RpcWrapper) GetWallet() *openwallet.Wallet {
	if len(w.AppID) == 0 {
		log.Error("Wrapper AppID is nil")
		return nil
	}
	if len(w.WalletID) == 0 {
		log.Error("Wrapper WalletID is nil")
		return nil
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		log.Error("Wrapper Get Mongo faild")
		return nil
	}
	defer mongo.Close()
	query := model.OwWallet{}
	if err := mongo.FindOne(sqlc.M(model.OwWallet{}).Eq("appID", w.AppID).Eq("walletID", w.WalletID).Eq("state", 1), &query); err != nil {
		log.Error(util.AddStr("Wrapper Find Wallet [", w.WalletID, "] faild"))
		return nil
	}
	if query.Id == 0 {
		log.Error(util.AddStr("Wrapper Wallet [", w.WalletID, "] Not Exist"))
		return nil
	}
	return &openwallet.Wallet{
		AppID:        query.AppID,
		WalletID:     query.WalletID,
		Alias:        query.Alias,
		Password:     query.Password,
		RootPub:      query.AuthKey,
		RootPath:     query.RootPath,
		KeyFile:      query.Keystore,
		IsTrust:      common.UIntToBool(uint64(query.IsTrust)),
		AccountIndex: int(query.AccountIndex),
	}
}

func (w *RpcWrapper) GetWalletByID(walletID string) (*openwallet.Wallet, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(walletID) == 0 {
		return nil, util.Error("Wrapper WalletID is nil")
	}
	if w.WalletID != walletID {
		return nil, util.Error("Wrapper WalletID Not Match")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := model.OwWallet{}
	if err := mongo.FindOne(sqlc.M(model.OwWallet{}).Eq("appID", w.AppID).Eq("walletID", walletID).Eq("state", 1), &query); err != nil {
		return nil, err
	}
	if query.Id == 0 {
		return nil, util.Error("Wrapper Wallet [", walletID, "] Not Exist")
	}
	return &openwallet.Wallet{
		AppID:        query.AppID,
		WalletID:     query.WalletID,
		Alias:        query.Alias,
		Password:     query.Password,
		RootPub:      query.AuthKey,
		RootPath:     query.RootPath,
		KeyFile:      query.Keystore,
		IsTrust:      common.UIntToBool(uint64(query.IsTrust)),
		AccountIndex: int(query.AccountIndex),
	}, nil
}

func (w *RpcWrapper) GetAssetsAccountInfo(accountID string) (*openwallet.AssetsAccount, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(accountID) == 0 {
		return nil, util.Error("Wrapper AccountID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}
	//if w.AccountID != accountID {
	//	return nil, util.Error("Wrapper AccountID Not Match")
	//}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := model.OwAccount{}
	if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("appID", w.AppID).Eq("accountID", accountID).Eq("symbol", strings.ToUpper(w.Symbol)).Eq("state", 1), &query); err != nil {
		return nil, err
	}
	if query.Id == 0 {
		return nil, util.Error("Wrapper Account [", accountID, "] Not Exist")
	}
	return query.ToAssetsAccount(), nil
}

func (w *RpcWrapper) GetAssetsAccountList(offset, limit int, cols ...interface{}) ([]*openwallet.AssetsAccount, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}
	sql := sqlc.M(model.OwAccount{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).Eq("state", 1)
	if limit > 0 {
		sql.Offset(int64(offset), int64(limit))
	} else {
		sql.Offset(int64(offset), int64(50))
	}
	if cols != nil && len(cols) > 0 {
		for k := 0; k < len(cols); k += 2 {
			key, ok := cols[k].(string)
			if !ok || len(key) == 0 {
				continue
			}
			sql.Eq(util.LowerFirst(key), cols[k+1])
		}
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := []*model.OwAccount{}
	if err := mongo.FindList(sql, &query); err != nil {
		return nil, err
	}
	assetAccounts := make([]*openwallet.AssetsAccount, 0)
	for e := range query {
		account := query[e]
		assetAccounts = append(assetAccounts, account.ToAssetsAccount())
	}
	return assetAccounts, nil
}

func (w *RpcWrapper) GetAssetsAccountByAddress(address string) (*openwallet.AssetsAccount, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}
	if len(address) == 0 {
		return nil, util.Error("Wrapper Address is nil")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := model.OwAddress{}
	if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("accountID", w.AppID).Eq("symbol", strings.ToUpper(w.Symbol)).Eq("address", address).Eq("state", 1), &query); err != nil {
		return nil, err
	}
	if query.Id == 0 {
		return nil, util.Error("Wrapper Address [", address, "] Not Exist")
	}
	account := model.OwAccount{}
	if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("appID", w.AppID).Eq("accountID", query.AccountID).Eq("symbol", query.Symbol).Eq("state", 1), &account); err != nil {
		return nil, err
	}
	if account.Id == 0 {
		return nil, util.Error("Wrapper Account [", query.AccountID, "] Not Exist")
	}
	return account.ToAssetsAccount(), nil
}

func (w *RpcWrapper) GetAddress(address string) (*openwallet.Address, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}
	if len(address) == 0 {
		return nil, util.Error("Wrapper Address is nil")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := model.OwAddress{}
	if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).Eq("address", address).Eq("state", 1), &query); err != nil {
		return nil, err
	}
	if query.Id == 0 {
		if w.Symbol == model.ETH { // ??????ETH??????????????????????????????TRUE???????????????
			if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("symbol", model.TRUE).Eq("address", address).Eq("state", 1), &query); err != nil {
				return nil, err
			}
		}
		if query.Id == 0 {
			return nil, util.Error("Wrapper Address [", address, "] Not Exist")
		}
	}
	return query.ToAddress(), nil
}

//??????ow_address_token???????????????????????????????????????????????????map
func(w RpcWrapper) getAllTokenAddressMap(query *sqlc.Cnd) (map[string]bool, error) {
	query.Limit(1, 100)
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()

	if _, err := mongo.Count(query); err != nil {
		return nil, util.Error("???????????????????????????????????????")
	}
	pagin := query.Pagination
	addrMap := make(map[string]bool, 0)
	for i := int64(1); i <= pagin.PageCount; i++ {
		addressList := []*model.OwAddressToken{}
		if err := mongo.FindList(query.Limit(i, pagin.PageSize), &addressList); err != nil {
			continue
		}

		for _, addr := range addressList {
			if _, exist := addrMap[addr.Address]; !exist {
				addrMap[addr.Address] = true
			}
		}
	}
	return addrMap, nil
}


func(w RpcWrapper) getTokenAddressesByQuery(query *sqlc.Cnd) ([]string, error) {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	addressList := []*model.OwAddressToken{}
	if err := mongo.FindList(query, &addressList); err != nil {
		return nil, util.Error("???????????????????????????????????????")
	}

	addressArray := make([]string, 0)
	for _, addr := range addressList {
		addressArray = append(addressArray, addr.Address)
	}
	return addressArray, nil
}

func(w RpcWrapper) getAddressesByQuery(query *sqlc.Cnd) ([]string, error) {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()

	addressList := []*model.OwAddress{}
	if err := mongo.FindList(query, &addressList); err != nil {
		return nil, util.Error("???????????????????????????????????????")
	}

	addressArray := make([]string, 0)
	for _, addr := range addressList {
		addressArray = append(addressArray, addr.Address)
	}

	return addressArray, nil
}

func (w *RpcWrapper) GetAddressListContainsBalance(offset, limit int, cols ...interface{}) ([]string, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}

	getTokenAddress := false
	sql      := sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).Eq("state", 1).NotEq("balance", "0")
	sqlToken := sqlc.M(model.OwAddressToken{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).Eq("state", 1).NotEq("balance", "0")

	if limit <= 0 {
		limit = 100
	}

	sql.Offset(int64(offset), int64(limit))
	sqlToken.Offset(int64(offset), int64(limit))

	if cols != nil && len(cols) > 0 {
		for k := 0; k < len(cols); k += 2 {
			key, ok := cols[k].(string)
			if !ok || len(key) == 0 {
				continue
			}
			if key == "ContractID" {
				getTokenAddress = true
				sqlToken.Eq(util.LowerFirst(key), cols[k+1])
				continue
			}
			sql.Eq(util.LowerFirst(key), cols[k+1])
			sqlToken.Eq(util.LowerFirst(key), cols[k+1])

		}
	}

	if getTokenAddress {
		return w.getTokenAddressesByQuery(sqlToken)
	} else {
		return w.getAddressesByQuery(sql)
	}
	return nil, fmt.Errorf("something error not match mode")
}

func (w *RpcWrapper) GetAddressList(offset, limit int, cols ...interface{}) ([]*openwallet.Address, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}

	getTokenAddress := false
	filterBalance := true// ??????????????????0????????? ??????true
	sql := sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("state", 1)
	sqlToken := sqlc.M(model.OwAddressToken{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).NotEq("balance", "0").Eq("state", 1)

	if cols != nil && len(cols) > 0 {
		for k := 0; k < len(cols); k += 2 {
			key, ok := cols[k].(string)
			if !ok || len(key) == 0 {
				continue
			}
			if key == "ContractID" {
				getTokenAddress = true
				sqlToken.Eq(util.LowerFirst(key), cols[k+1])
				continue
			}

			if key == FilterBalanceKey {
				filterBalance = cols[k+1].(bool)
			}

			if key != FilterBalanceKey {//???sql??????
				sql.Eq(util.LowerFirst(key), cols[k+1])
				sqlToken.Eq(util.LowerFirst(key), cols[k+1])
			}
		}
	}

	if getTokenAddress {
		//var ret []*openwallet.Address
		ret := make([]*openwallet.Address, 0)
		result, err := w.getAddressListBySymbol(w.Symbol, offset, limit, sql)
		if err != nil {
			return nil, err
		}

		//??????ow_address_token???????????????????????????????????????????????????map
		addressTokenMap, err := w.getAllTokenAddressMap(sqlToken)
		if err != nil {
			return nil, err
		}
		for _, v := range result {
			if _, exist := addressTokenMap[v.Address]; exist {
				ret = append(ret, v)
			}
		}
		log.Info("token address symbol:" + w.Symbol + "," + ",address token map size:", len(addressTokenMap), ",result size:", len(result), ",ret size:", len(ret))
		return ret, nil
	} else {
		result, err := w.getAddressListBySymbol(w.Symbol, offset, limit, sql)
		if err != nil {
			return nil, err
		}
		if len(result) == 0 && w.Symbol == model.ETH { // ??????ETH??????????????????????????????TRUE???????????????
			result, err = w.getAddressListBySymbol(model.TRUE, offset, limit, sql)
			if err != nil {
				return nil, err
			}
		}

		if !filterBalance {//???????????????0??????
			return result, nil
		}
		addrHaveBalance := make([]*openwallet.Address, 0)
		for _, a := range result {
			if a.Balance != "0" {
				addrHaveBalance = append(addrHaveBalance, a)
			}
		}

		return addrHaveBalance, nil
	}
	return nil, fmt.Errorf("something error not match mode")
}

func (w *RpcWrapper) getAddressListBySymbol(symbol string, offset, limit int, sql *sqlc.Cnd) ([]*openwallet.Address, error) {
	if limit > 0 {
		sql.Offset(int64(offset), int64(limit))
	} else {
		sql.Offset(int64(offset), int64(50))
	}
	sql.Eq("symbol", symbol)
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := []*model.OwAddress{}
	if err := mongo.FindList(sql, &query); err != nil {
		return nil, err
	}
	addresses := make([]*openwallet.Address, 0)
	for _, v := range query {
		addresses = append(addresses, v.ToAddress())
	}
	return addresses, nil
}

func (w *RpcWrapper) UnlockWallet(password string, time time.Duration) error {
	if w.AppID == "" {
		log.Error("Wrapper AppID is nil")
		return errors.New("Wrapper AppID is nil")
	}
	key, err := w.HDKey(password)
	if err != nil {
		return err
	}
	w.key = key
	return nil
}

func (w *RpcWrapper) HDKey(password ...string) (*hdkeystore.HDKey, error) {
	if w.AppID == "" {
		log.Error("Wrapper AppID is nil")
		return nil, errors.New("Wrapper AppID is nil")
	}
	if w.WalletID == "" {
		log.Error("Wrapper AppID is nil")
		return nil, errors.New("Wrapper AppID is nil")
	}

	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := model.OwWallet{}
	if err := mongo.FindOne(sqlc.M(model.OwWallet{}).Eq("appID", w.AppID).Eq("walletID", w.WalletID).Eq("state", 1), &query); err != nil {
		log.Error(util.AddStr("Wrapper Find Wallet [", w.WalletID, "] faild"))
		return nil, err
	}
	if query.Id == 0 {
		log.Error(util.AddStr("Wrapper Wallet [", w.WalletID, "] Not Exist"))
		return nil, err
	}
	if query.IsTrust == 0 {
		log.Error(util.AddStr("Wrapper Wallet [", w.WalletID, "] Is Not Trust"))
		return nil, err
	}
	key, err := hdkeystore.DecryptHDKey([]byte(query.Keystore), query.Password)
	if err != nil {
		return nil, err
	}
	return key, nil
}

//???????????????????????????
func (w *RpcWrapper) SetAddressExtParam(address string, key string, val interface{}) error {
	if len(w.AppID) == 0 {
		return util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return util.Error("Wrapper Symbol is nil")
	}
	if len(address) == 0 {
		return util.Error("Wrapper Address is nil")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	query := model.OwAddress{}
	if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).Eq("address", address).Eq("state", 1), &query); err != nil {
		return err
	}
	if query.Id == 0 {
		return util.Error("Wrapper Address[", address, "] not exist")
	}
	//??????map????????????
	if query.ExtParam == nil {
		query.ExtParam = make(map[string]interface{}, 0)
	}
	query.ExtParam[key] = val
	if err := mongo.Update(&query); err != nil {
		return err
	}
	return nil
}

//???????????????????????????
func (w *RpcWrapper) GetAddressExtParam(address string, key string) (interface{}, error) {
	if len(w.AppID) == 0 {
		return nil, util.Error("Wrapper AppID is nil")
	}
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}
	if len(address) == 0 {
		return nil, util.Error("Wrapper Address is nil")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	query := model.OwAddress{}
	if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("appID", w.AppID).Eq("symbol", w.Symbol).Eq("address", address).Eq("state", 1), &query); err != nil {
		return nil, err
	}
	if query.Id == 0 {
		return nil, util.Error("Wrapper Address[", address, "] not exist")
	}
	if query.ExtParam != nil && query.ExtParam[key] != nil {
		return query.ExtParam[key], nil
	}
	return nil, nil
}

// ???????????????????????????
func (w *RpcWrapper) GetTransactionByTxID(txid, symbol string) ([]*openwallet.Transaction, error) {
	if len(w.Symbol) == 0 {
		return nil, util.Error("Wrapper Symbol is nil")
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	tradelogTmp := []*model.OwTradeLogTmp{}
	if err := mongo.FindList(sqlc.M(model.OwTradeLogTmp{}).Eq("txid", txid).Eq("symbol", symbol), &tradelogTmp); err != nil {
		return nil, err
	}
	if len(tradelogTmp) > 0 {
		txList := make([]*openwallet.Transaction, 0, len(tradelogTmp))
		for _, v := range tradelogTmp {
			tx := &openwallet.Transaction{}
			tx.WxID = v.Wxid
			tx.TxID = v.Txid
			tx.AccountID = v.AccountID
			coin := openwallet.Coin{Symbol: v.Symbol, ContractID: v.ContractID}
			if v.IsContract == 0 {
				coin.Contract = openwallet.SmartContract{}
			} else {
				coin.IsContract = true
				coin.Contract = openwallet.SmartContract{Symbol: v.Symbol, ContractID: v.ContractID, Token: v.ContractName, Address: v.ContractAddr}
			}
			tx.Coin = coin
			for i, av := range v.FromAddress {
				tx.From = append(tx.From, util.AddStr(av, ":", v.FromAddressV[i]))
			}
			for i, av := range v.ToAddress {
				tx.To = append(tx.To, util.AddStr(av, ":", v.ToAddressV[i]))
			}
			tx.Amount = v.Amount
			tx.Decimal = int32(v.Decimals)
			tx.TxType = uint64(v.TxType)
			tx.TxAction = v.TxAction
			tx.Confirm = v.Confirm
			tx.BlockHash = v.BlockHash
			tx.BlockHeight = uint64(v.BlockHeight)
			if v.IsMemo > 0 {
				tx.IsMemo = true
			}
			tx.Memo = v.Memo
			tx.Fees = v.Fees
			tx.SubmitTime = v.SubmitTime
			tx.ConfirmTime = v.ConfirmTime
			tx.Status = v.Success
			txList = append(txList, tx)
		}
		return txList, nil
	}
	tradelog := []*model.OwTradeLog{}
	if err := mongo.FindList(sqlc.M(model.OwTradeLog{}).Eq("txid", txid).Eq("symbol", symbol), &tradelog); err != nil {
		return nil, err
	}
	if len(tradelog) > 0 {
		txList := make([]*openwallet.Transaction, 0, len(tradelog))
		for _, v := range tradelog {
			tx := &openwallet.Transaction{}
			tx.WxID = v.Wxid
			tx.TxID = v.Txid
			tx.AccountID = v.AccountID
			coin := openwallet.Coin{Symbol: v.Symbol, ContractID: v.ContractID}
			if v.IsContract == 0 {
				coin.Contract = openwallet.SmartContract{}
			} else {
				coin.IsContract = true
				coin.Contract = openwallet.SmartContract{Symbol: v.Symbol, ContractID: v.ContractID, Token: v.ContractName, Address: v.ContractAddr}
			}
			tx.Coin = coin
			for i, av := range v.FromAddress {
				tx.From = append(tx.From, util.AddStr(av, ":", v.FromAddressV[i]))
			}
			for i, av := range v.ToAddress {
				tx.From = append(tx.From, util.AddStr(av, ":", v.ToAddress[i]))
			}
			tx.Amount = v.Amount
			tx.Decimal = int32(v.Decimals)
			tx.TxType = uint64(v.TxType)
			tx.TxAction = v.TxAction
			tx.Confirm = v.Confirm
			tx.BlockHash = v.BlockHash
			tx.BlockHeight = uint64(v.BlockHeight)
			if v.IsMemo > 0 {
				tx.IsMemo = true
			}
			tx.Memo = v.Memo
			tx.Fees = v.Fees
			tx.SubmitTime = v.SubmitTime
			tx.ConfirmTime = v.ConfirmTime
			tx.Status = v.Success
			txList = append(txList, tx)
		}
		return txList, nil
	}
	return make([]*openwallet.Transaction, 0), nil
}

func NewWrapper(appID, walletID string, accountID string, symbol string) *RpcWrapper {
	wrapper := RpcWrapper{
		AppID:     appID,
		WalletID:  walletID,
		AccountID: accountID,
		Symbol:    symbol,
	}
	return &wrapper
}
