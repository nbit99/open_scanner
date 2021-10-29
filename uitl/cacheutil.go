package tradeutil

import (
	"github.com/blocktree/go-openw-server/open_base/model"
	"github.com/godaddy-x/jorm/cache/redis"
	"github.com/godaddy-x/jorm/exception"
	"github.com/godaddy-x/jorm/sqlc"
	"github.com/godaddy-x/jorm/sqld"
	"github.com/godaddy-x/jorm/util"
)

const (
	cache_expire = 3600
)

func GetObj(key string, value interface{}, expire int, call func(mongo *sqld.MGOManager) (interface{}, error)) error {
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	b, err := client.Get(key, value)
	if err != nil {
		return err
	}
	if b {
		return nil
	}
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	val, err := call(mongo)
	if val == nil {
		return nil
	}
	if expire > 0 {
		if err := client.Put(key, val, expire); err != nil {
			return err
		}
	} else {
		if err := client.Put(key, val, expire); err != nil {
			return err
		}
	}
	return nil
}

func PutObj(key string, value interface{}, expire int) error {
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	if expire > 0 {
		if err := client.Put(key, value, expire); err != nil {
			return err
		}
	} else {
		if err := client.Put(key, value, expire); err != nil {
			return err
		}
	}
	return nil
}

func GetApp(appid string) (*model.OwApp, error) {
	app := model.OwApp{}
	if err := GetObj(util.AddStr("cache.tx.app.", appid), &app, cache_expire, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwApp{}).Eq("appid", appid).Eq("state", 1), &app); err != nil {
			return nil, err
		}
		if app.Id == 0 {
			return nil, nil
		}
		return &app, nil
	}); err != nil {
		return nil, util.Error("应用[", appid, "]读取缓存异常: ", err.Error())
	}
	return &app, nil
}

func SetApp(appid string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	app := model.OwApp{}
	if err := mongo.FindOne(sqlc.M(model.OwApp{}).Eq("appid", appid).Eq("state", 1), &app); err != nil {
		return err
	}
	if app.Id == 0 {
		return nil
	}
	return PutObj(util.AddStr("cache.tx.app.", appid), &app, cache_expire)
}

func GetSymbol(coin string) (*model.OwSymbol, error) {
	symbol := model.OwSymbol{}
	if err := GetObj(util.AddStr("cache.tx.coin.", coin), &symbol, cache_expire, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwSymbol{}).Eq("coin", coin).Eq("state", 1), &symbol); err != nil {
			return nil, err
		}
		if symbol.Id == 0 {
			return nil, nil
		}
		return &symbol, nil
	}); err != nil {
		return nil, util.Error("币种[", coin, "]读取缓存异常: ", err.Error())
	}
	return &symbol, nil
}

func SetSymbol(coin string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	symbol := model.OwSymbol{}
	if err := mongo.FindOne(sqlc.M(model.OwSymbol{}).Eq("coin", coin).Eq("state", 1), &symbol); err != nil {
		return err
	}
	if symbol.Id == 0 {
		return nil
	}
	return PutObj(util.AddStr("cache.tx.coin.", coin), &symbol, cache_expire)
}

func GetContract(contractID string, dst interface{}) (*model.OwContract, error) {
	contract := model.OwContract{}
	if err := GetObj(util.AddStr("cache.tx.contract.", contractID), &contract, cache_expire, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwContract{}).Eq("contractID", contractID).Eq("state", 1), &contract); err != nil {
			return nil, err
		}
		if contract.Id == 0 {
			return nil, nil
		}
		return &contract, nil
	}); err != nil {
		return nil, util.Error("合约[", contractID, "]读取缓存异常: ", err.Error())
	}
	if dst != nil && contract.Id > 0 {
		if err := util.DeepCopy(dst, &contract); err != nil {
			return nil, err
		}
	}
	return &contract, nil
}

func SetContract(contractID string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	contract := model.OwContract{}
	if err := mongo.FindOne(sqlc.M(model.OwContract{}).Eq("contractID", contractID).Eq("state", 1), &contract); err != nil {
		return err
	}
	if contract.Id == 0 {
		return nil
	}
	return PutObj(util.AddStr("cache.tx.contract.", contractID), &contract, cache_expire)
}

func GetAccount(walletID, accountID string) (*model.OwAccount, error) {
	account := model.OwAccount{}
	if err := GetObj(util.AddStr("cache.tx.account.", util.MD5(walletID, accountID)), &account, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("walletID", walletID).Eq("accountID", accountID).Eq("state", 1), &account); err != nil {
			return nil, err
		}
		if account.Id == 0 {
			return nil, nil
		}
		return &account, nil
	}); err != nil {
		return nil, util.Error("账号[", accountID, "]读取缓存异常: ", err.Error())
	}
	return &account, nil
}

func GetAccountByAppID(appID, accountID string) (*model.OwAccount, error) {
	account := model.OwAccount{}
	if err := GetObj(util.AddStr("cache.tx.account.", util.MD5(appID, accountID)), &account, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("appID", appID).Eq("accountID", accountID).Eq("state", 1), &account); err != nil {
			return nil, err
		}
		if account.Id == 0 {
			return nil, nil
		}
		return &account, nil
	}); err != nil {
		return nil, util.Error("账号[", accountID, "]读取缓存异常: ", err.Error())
	}
	return &account, nil
}

func GetAddress(accountID, addressV string) (*model.OwAddress, error) {
	address := model.OwAddress{}
	if err := GetObj(util.AddStr("cache.tx.address.", util.MD5(accountID, addressV)), &address, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwAddress{}).Eq("accountID", accountID).Eq("address", addressV).Eq("state", 1), &address); err != nil {
			return nil, err
		}
		if address.Id == 0 {
			return nil, nil
		}
		return &address, nil
	}); err != nil {
		return nil, util.Error("地址[", addressV, "]读取缓存异常: ", err.Error())
	}
	return &address, nil
}

func SetSetting(platform string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	setting := model.OwSetting{}
	if err := mongo.FindOne(sqlc.M(model.OwSetting{}).Eq("platform", platform).Eq("state", 1), &setting); err != nil {
		return err
	}
	if setting.Id == 0 {
		return nil
	}
	return PutObj(util.AddStr("platform.setting.", platform), &setting, cache_expire)
}

func GetSetting(platform string) (*model.OwSetting, error) {
	setting := model.OwSetting{}
	if err := GetObj(util.AddStr("platform.setting.", platform), &setting, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwSetting{}).Eq("platform", platform).Eq("state", 1), &setting); err != nil {
			return nil, err
		}
		if setting.Id == 0 {
			return nil, nil
		}
		return &setting, nil
	}); err != nil {
		return nil, util.Error("程序开关[", platform, "]读取缓存异常: ", err.Error())
	}
	return &setting, nil
}

func GetAccountToken(walletID, accountID, contractID string) (*model.OwAccountToken, error) {
	accountToken := model.OwAccountToken{}
	if err := GetObj(util.AddStr("cache.tx.account.token.", util.MD5(util.AddStr(walletID, accountID, contractID))), &accountToken, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwAccountToken{}).Eq("walletID", walletID).Eq("accountID", accountID).Eq("contractID", contractID).Eq("state", 1), &accountToken); err != nil {
			return nil, err
		}
		if accountToken.Id == 0 {
			return nil, nil
		}
		return &accountToken, nil
	}); err != nil {
		return nil, util.Error("账号Token[", accountID, ";", contractID, "]读取缓存异常: ", err.Error())
	}
	return &accountToken, nil
}

func GetAddressToken(walletID, accountID, addressV, contractID string) (*model.OwAddressToken, error) {
	addressToken := model.OwAddressToken{}
	if err := GetObj(util.AddStr("cache.tx.address.token.", util.MD5(util.AddStr(walletID, accountID, addressV, contractID))), &addressToken, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwAddressToken{}).Eq("walletID", walletID).Eq("accountID", accountID).Eq("address", addressV).Eq("contractID", contractID).Eq("state", 1), &addressToken); err != nil {
			return nil, err
		}
		if addressToken.Id == 0 {
			return nil, nil
		}
		return &addressToken, nil
	}); err != nil {
		return nil, util.Error("地址Token[", accountID, ";", contractID, "]读取缓存异常: ", err.Error())
	}
	return &addressToken, nil
}

func GetBindDevice(appid, pid string) (*model.OwAppDevice, error) {
	appDevice := model.OwAppDevice{}
	if err := GetObj(util.AddStr("cache.tx.device.", util.MD5(appid, pid)), &appDevice, cache_expire, func(mongo *sqld.MGOManager) (interface{}, error) {
		if err := mongo.FindOne(sqlc.M(model.OwAppDevice{}).Eq("appID", appid).Eq("pid", pid).Eq("usestate", 1).Eq("state", 1), &appDevice); err != nil {
			return nil, err
		}
		if appDevice.Id == 0 {
			return nil, nil
		}
		return &appDevice, nil
	}); err != nil {
		return nil, util.Error("应用设备[", appid, ";", pid, "]读取缓存异常: ", err.Error())
	}
	return &appDevice, nil
}

func SetBindDevice(appid, pid string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	appDevice := model.OwAppDevice{}
	if err := mongo.FindOne(sqlc.M(model.OwAppDevice{}).Eq("appID", appid).Eq("pid", pid).Eq("state", 1), &appDevice); err != nil {
		return ex.Try{ex.DATA, ex.DATA_R_ERR, err, nil}
	}
	if appDevice.Id == 0 {
		return nil
	}
	return PutObj(util.AddStr("cache.tx.device.", util.MD5(appid, pid)), &appDevice, cache_expire)
}

func GetIgrtx(appid, symbol, txid string) (*model.OwIgrtx, error) {
	igrtx := model.OwIgrtx{}
	if err := GetObj(util.AddStr("cache.tx.igrtx.", util.MD5(util.AddStr(appid, symbol, txid))), &igrtx, 0, func(mongo *sqld.MGOManager) (interface{}, error) {
		return &igrtx, nil
	}); err != nil {
		return nil, util.Error("忽略交易单[", appid, ";", txid, "]读取缓存异常: ", err.Error())
	}
	return &igrtx, nil
}

func SetIgrtx(appid, symbol, txid string) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return err
	}
	defer mongo.Close()
	igrtx := model.OwIgrtx{}
	if err := mongo.FindOne(sqlc.M(model.OwIgrtx{}).Eq("appID", appid).Eq("symbol", symbol).Eq("txid", txid).Eq("state", 1), &igrtx); err != nil {
		return ex.Try{ex.DATA, ex.DATA_R_ERR, err, nil}
	}
	if igrtx.Id == 0 {
		return nil
	}
	return PutObj(util.AddStr("cache.tx.igrtx.", util.MD5(util.AddStr(appid, symbol, txid))), &igrtx, 0)
}
