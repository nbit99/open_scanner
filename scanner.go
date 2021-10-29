package open_scanner

import (
	"errors"
	"flag"
	"fmt"
	"github.com/astaxie/beego/config"
	"github.com/astaxie/beego/logs"
	"github.com/blocktree/go-openw-server/open_base/major"
	"github.com/blocktree/go-openw-server/open_base/model"
	"github.com/blocktree/go-openw-server/open_scanner/rpc"
	"github.com/blocktree/go-openw-server/open_scanner/rpc/dto"
	"github.com/blocktree/openwallet/v2/assets"
	"github.com/blocktree/openwallet/v2/common/file"
	"github.com/blocktree/openwallet/v2/log"
	"github.com/blocktree/openwallet/v2/openwallet"
	"github.com/godaddy-x/jorm/amqp"
	"github.com/godaddy-x/jorm/cache/redis"
	"github.com/godaddy-x/jorm/consul"
	log2 "github.com/godaddy-x/jorm/log"
	"github.com/godaddy-x/jorm/sqlc"
	"github.com/godaddy-x/jorm/sqld"
	"github.com/godaddy-x/jorm/util"
	"github.com/shopspring/decimal"
	"path/filepath"
	"strings"
	"time"
)

const (
	exchange = "tx.exchange"
	queue    = "tx.queue."
)

var Pause = flag.Int64("p", 0, "")

func InitWallet(adapter openwallet.AssetsAdapter, walletapi service.WalletApiService, server *OpenWScanner) {
	major.InitDB()
	major.InitLog("scanner_" + strings.ToLower(server.Symbol))
	server.Adapter = adapter
	server.Walletapi = walletapi
	server.Pause = *Pause
	server.StartWallet()
	server.AddRegistration()
}

type OpenWScanner struct {
	InitHeight   int64
	ReHeight     int64
	Symbol       string
	Adapter      openwallet.AssetsAdapter
	Walletapi    service.WalletApiService
	BlockScanner openwallet.BlockScanner
	Pause        int64
	DbPath       string
	DbName       string
}

func (o *OpenWScanner) StartWallet() {
	symbol := o.Symbol
	adapter := o.Adapter
	if err := major.LoadConsulSymbol(symbol); err != nil {
		panic(err)
	}
	assets.RegAssets(symbol, adapter)
	coin, err := ValidConfigSymbol(symbol)
	if err != nil {
		log.Error(symbol, err.Error())
		return
	}
	assetsMgr, err := GetAssetsManager(symbol)
	if err != nil {
		log.Error(symbol, "is not support")
		return
	}
	if c, err := o.LoadConfig(symbol); err != nil {
		return
	} else {
		assetsMgr.LoadAssetsConfig(c) //读取对应配置(留下疑问，暂时一个币种只能支持一个机器)
	}
	// 加载费率缓存
	if err := o.CacheFreerate(symbol); err != nil {
		log.Error("cache [", symbol, "] freerate error: ", err.Error())
		return
	}
	log.Notice(symbol, " Wallet Manager Load Successfully.")
	if o.Pause == 0 {
		//设置日志信息
		logPath := major.InitNetwork().ScanLogPath
		loggerSetting := assetsMgr.GetAssetsLogger()
		logDir := filepath.Join(logPath)
		file.MkdirAll(logDir)
		logFile := filepath.Join(logPath, util.AddStr(symbol, ".log"))
		logConfig := fmt.Sprintf(`{"filename":"%s"}`, logFile)
		// 设置日志级别
		loggerSetting.SetLevel(log.LevelInformational)
		loggerSetting.SetLogger(logs.AdapterFile, logConfig)
		loggerSetting.SetLogger(logs.AdapterConsole, logConfig)
		//扫块启动
		scanner := assetsMgr.GetBlockScanner()
		if scanner == nil {
			log.Error(symbol, "is not support [", symbol, "] block scan")
			return
		}
		if scanner.SupportBlockchainDAI() {
			file.MkdirAll(o.DbPath)
			dai, err := openwallet.NewBlockchainLocal(filepath.Join(o.DbPath, o.DbName), false)
			if err != nil {
				log.Error("NewBlockchainLocal err: %v", err)
				return
			}
			scanner.SetBlockchainDAI(dai)
		}
		o.BlockScanner = scanner
		//加载地址时，暂停区块扫描
		scanner.Pause()
		//扫块读取是否我们的地址,GetSourceKeyByAddress 获取地址对应的数据源标识
		scanner.SetBlockScanTargetFunc(scanTargetFunc(symbol))
		scanner.SetBlockScanTargetFuncV2(scanTargetFuncV2)
		if o.InitHeight > 0 {
			scanner.SetRescanBlockHeight(uint64(o.InitHeight))
		}
		if o.ReHeight > 0 {
			scanner.ScanBlock(uint64(o.ReHeight))
		}
		// 设置walletapi接口实现类
		scanner.SetBlockScanWalletDAI(NewWrapper("", "", "", symbol))
		//添加观测者到区块扫描器
		scanner.AddObserver(o)
		if coin.BlockStop == 0 {
			log.Info(symbol, " 扫块启动成功(运行中)...")
			scanner.Run()
		} else {
			log.Info(symbol, " 扫块启动成功(暂停中)...")
		}
		//o.CheckBlockScannerState()
	}
}

func (o *OpenWScanner) CheckBlockScannerState() error {
	go func() {
		pause := false
		for ; ; {
			time.Sleep(20 * time.Second)
			mongo, err := new(sqld.MGOManager).Get()
			if err != nil {
				log.Error("检测币种开关状态获取mongo失败")
				continue
			}
			defer mongo.Close()
			conf := model.OwSetting{}
			if err := mongo.FindOne(sqlc.M(model.OwSetting{}).Eq("platform", "scanner"), &conf); err != nil {
				log.Error("检测币种开关状态获取数据失败失败")
				continue
			}
			if conf.Id == 0 {
				continue
			}
			if conf.IsOff > 0 {
				if !pause {
					o.BlockScanner.Pause()
					pause = true
					log.Warn(util.AddStr("[", o.Symbol, "]扫块器已暂停"))
				}
			} else {
				if pause {
					o.BlockScanner.Restart()
					pause = false
					log.Warn(util.AddStr("[", o.Symbol, "]扫块器已启动"))
				}
			}
		}
	}()
	return nil
}

// 扫块器回调函数
func scanTargetFunc(symbol string) func(target openwallet.ScanTarget) (string, bool) {
	return func(target openwallet.ScanTarget) (string, bool) {
		//如果余额模型是地址，查找地址表
		if target.BalanceModelType == openwallet.BalanceModelTypeAddress {
			if accountID, err := getAccountIDByAddress(target.Address, strings.ToUpper(symbol)); err != nil || accountID == "" {
				return "", false
			} else {
				return accountID, true
			}
		} else {
			//如果余额模型是账户，用别名操作账户的别名
			if accountID, err := getAccountIDByAlias(target.Alias, strings.ToUpper(symbol)); err != nil || accountID == "" {
				return "", false
			} else {
				return accountID, true
			}
		}
	}
}

// 扫块器回调函数V2 0: 账户地址，1：账户别名，2：合约地址，3：合约别名，4：地址公钥
func scanTargetFuncV2(target openwallet.ScanTargetParam) openwallet.ScanTargetResult {
	if target.ScanTargetType == 0 { // 地址模型
		if accountID, err := getAccountIDByAddress(target.ScanTarget, strings.ToUpper(target.Symbol)); err != nil || accountID == "" {
			return openwallet.ScanTargetResult{SourceKey: "", Exist: false}
		} else {
			return openwallet.ScanTargetResult{SourceKey: accountID, Exist: true}
		}
	} else if target.ScanTargetType == 1 { // 账户模型
		if accountID, err := getAccountIDByAlias(target.ScanTarget, strings.ToUpper(target.Symbol)); err != nil || accountID == "" {
			return openwallet.ScanTargetResult{SourceKey: "", Exist: false}
		} else {
			return openwallet.ScanTargetResult{SourceKey: accountID, Exist: true}
		}
	} else if target.ScanTargetType == 2 || target.ScanTargetType == 3 {
		mongo, err := new(sqld.MGOManager).Get()
		if err != nil {
			log2.Error("扫块器回调查询合约 - 获取mongo失败", 0, log2.AddError(err))
			return openwallet.ScanTargetResult{SourceKey: "", Exist: false}
		}
		defer mongo.Close()
		contract := model.OwContract{}
		if err := mongo.FindOne(sqlc.M(model.OwContract{}).Eq("symbol", target.Symbol).Eq("address", target.ScanTarget).Eq("state", 1), &contract); err != nil {
			log2.Error("扫块器回调查询合约 - 获取数据失败", 0, log2.AddError(err))
			return openwallet.ScanTargetResult{SourceKey: "", Exist: false}
		}
		if contract.Id == 0 {
			return openwallet.ScanTargetResult{SourceKey: "", Exist: false}
		}
		smart := &openwallet.SmartContract{
			ContractID: contract.ContractID,
			Symbol:     contract.Symbol,
			Address:    contract.Address,
			Token:      contract.Token,
			Protocol:   contract.Protocol,
			Name:       contract.Name,
			Decimals:   uint64(contract.Decimals),
		}
		smart.SetABI(contract.ABI)
		return openwallet.ScanTargetResult{SourceKey: contract.ContractID, Exist: true, TargetInfo: smart}
	} else if target.ScanTargetType == 4 {

	}
	return openwallet.ScanTargetResult{}
}

func (o *OpenWScanner) CacheFreerate(symbol string) error {
	result := map[string]interface{}{
		"symbol": symbol,
	}
	if symbol == "TRX" {
		result["feeRate"] = ""
		result["unit"] = ""
	} else {
		dreq := &dto.GetRawTransactionFeeRateReq{symbol}
		dresp := &dto.GetRawTransactionFeeRateResp{}
		o.Walletapi.GetRawTransactionFeeRate(dreq, dresp)
		result["feeRate"] = dresp.FeeRate
		result["unit"] = dresp.Unit
	}
	redis, err := new(cache.RedisManager).Client()
	if err != nil {
		return err
	}
	if err := redis.Put(util.AddStr("fr_", symbol), &result); err != nil {
		return err
	}
	return nil
}

func (o *OpenWScanner) AddRegistration() {
	// 注册服务
	consulx, err := new(consul.ConsulManager).Client(o.Symbol)
	if err != nil {
		panic(err)
	}
	tag := o.Symbol + "钱包RPC服务"
	// 移除RPC服务
	consulx.ClearTagService(tag)
	// 注册RPC服务
	consulx.AddRegistration(tag, o.Walletapi)
	// 启动RPC服务
	consulx.StartListenAndServe()
	major.ForeverWait(o.Symbol + "钱包RPC服务启动成功")
}

//BlockScanNotify 新区块扫描完成通知
func (o *OpenWScanner) BlockScanNotify(header *openwallet.BlockHeader) error {
	ret, sig, err := major.GenMQDataSig(header)
	if err != nil {
		log2.Warn(err.Error(), 0, log2.Any("header", header))
		return nil
	}
	client, err := new(rabbitmq.PublishManager).Client()
	if err != nil {
		log.Error(err.Error())
	}
	if err := client.Publish(rabbitmq.MsgData{Exchange: exchange, Queue: queue + o.Symbol, Type: 1, Content: ret, Signature: sig}); err != nil {
		log2.Error("区块数据发送MQ异常", 0, log2.String("symbol", o.Symbol), log2.String("exchange", exchange), log2.String("queue", queue+o.Symbol), log2.Any("content", header), log2.AddError(err))
	}
	return nil
}

//BlockExtractDataNotify 区块提取结果通知
func (o *OpenWScanner) BlockExtractDataNotify(sourceKey string, data *openwallet.TxExtractData) error {
	//jv, _ := util.ObjectToJson(data)
	//fmt.Println("test------", sourceKey, jv)
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return errors.New("mongo create err")
	}
	defer mongo.Close()
	unusual := false
	account := model.OwAccount{}
	contract := model.OwContract{}
	if err := mongo.FindOne(sqlc.M(model.OwAccount{}).Eq("accountID", sourceKey).Eq("state", 1), &account); err != nil {
		return err
	}
	if account.Id == 0 {
		if err := mongo.FindOne(sqlc.M(model.OwContract{}).Eq("contractID", sourceKey).Eq("state", 1), &contract); err != nil {
			return err
		}
		if contract.Id > 0 {
			unusual = true
		}
	}
	if account.Id == 0 && contract.Id == 0 {
		return util.Error("Wrapper Account or Contract[", sourceKey, "] Not Exist")
	}
	if data.Transaction != nil {
		amount := decimal.NewFromFloat(0)
		if data.TxInputs != nil {
			for _, v := range data.TxInputs {
				if v.Amount == "" {
					continue
				}
				inputAmount, err := decimal.NewFromString(v.Amount)
				if err != nil {
					return util.Error("input get amount error!")
				}
				amount = amount.Sub(inputAmount)
			}
		}
		if data.TxOutputs != nil {
			for _, v := range data.TxOutputs {
				if v.Amount == "" {
					continue
				}
				outputAmount, err := decimal.NewFromString(v.Amount)
				if err != nil {
					return util.Error("output get amount error!")
				}
				amount = amount.Add(outputAmount)
			}
		}
		data.Transaction.Amount = amount.String()
		var result map[string]interface{}
		if unusual { // 智能合约交易单
			//trade_sec := model.OwTrade{}
			//if err := mongo.FindOne(sqlc.M(model.OwTrade{}).Eq("txID", data.Transaction.TxID).Eq("reqtype", 2), &trade_sec); err != nil {
			//	return util.Error("查询广播交易记录失败: [", sourceKey, "-", data.Transaction.TxID, "],", err.Error())
			//}
			//if trade_sec.Id == 0 {
			//	return util.Error("查询广播交易记录无效: [", sourceKey, "-", data.Transaction.TxID, "],", err.Error())
			//}
			result = map[string]interface{}{
				"appID":      contract.ContractID,
				"walletID":   contract.ContractID,
				"accountID":  contract.ContractID,
				"dataType":   2,
				"content":    data.Transaction,
				"inputs":     data.TxInputs,
				"outputs":    data.TxOutputs,
				"contractID": contract.ContractID,
			}
		} else { // 普通交易单
			result = map[string]interface{}{
				"appID":      account.AppID,
				"walletID":   account.WalletID,
				"accountID":  account.AccountID,
				"dataType":   2,
				"content":    data.Transaction,
				"inputs":     data.TxInputs,
				"outputs":    data.TxOutputs,
				"contractID": "",
			}
		}
		ret, sig, err := major.GenMQDataSig(result)
		if err != nil {
			log2.Warn(err.Error(), 0, log2.Any("content", result))
			return nil
		}
		client, err := new(rabbitmq.PublishManager).Client()
		if err != nil {
			log2.Error("获取mq连接失败", 0, log2.AddError(err))
		}
		if err := client.Publish(rabbitmq.MsgData{Exchange: exchange, Queue: queue + o.Symbol, Type: 2, Content: ret, Signature: sig}); err != nil {
			log2.Error("发送MQ数据失败", 0, log2.String("appid", account.AppID), log2.String("exchange", exchange), log2.String("queue", queue+o.Symbol), log2.Any("content", result))
		}
	}
	return nil
}

// 提取智能合约交易单
func (o *OpenWScanner) BlockExtractSmartContractDataNotify(sourceKey string, data *openwallet.SmartContractReceipt) error {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return errors.New("mongo create err")
	}
	defer mongo.Close()
	contract := model.OwContract{}
	if err := mongo.FindOne(sqlc.M(model.OwContract{}).Eq("contractID", sourceKey).Eq("state", 1), &contract); err != nil {
		return err
	}
	if contract.Id == 0 {
		return util.Error("Wrapper Contract [", sourceKey, "] Not Exist")
	}
	//info, _ := util.ObjectToJson(data)
	//fmt.Println("BlockExtractSmartContractDataNotify------", info)
	ret, sig, err := major.GenMQDataSig(data)
	if err != nil {
		log2.Warn(err.Error(), 0, log2.Any("content", data))
		return nil
	}
	client, err := new(rabbitmq.PublishManager).Client()
	if err != nil {
		log2.Error("获取mq连接失败", 0, log2.AddError(err))
	}
	if err := client.Publish(rabbitmq.MsgData{Exchange: exchange, Queue: queue + "receipt", Type: 3, Content: ret, Signature: sig}); err != nil {
		log2.Error("发送MQ数据失败", 0, log2.String("exchange", exchange), log2.String("queue", queue+"receipt"), log2.Any("content", data))
	}
	return nil
}

func genDataSig(result interface{}) (string, string, error) {
	str, err := util.ObjectToJson(result)
	if err != nil || len(str) == 0 {
		return "", "", util.Error("区块/交易单数据转换JSON失败")
	}
	ret := util.Base64URLEncode(str)
	if len(str) == 0 {
		return "", "", util.Error("区块/交易单数据base64编码失败")
	}
	sig := util.MD5(ret, major.InitNetwork().MQSecretKey)
	return ret, sig, nil
}

func ValidConfigSymbol(symbol string) (*model.OwSymbol, error) {
	mongo, err := new(sqld.MGOManager).Get()
	if err != nil {
		return nil, err
	}
	defer mongo.Close()
	find := model.OwSymbol{}
	if err := mongo.FindOne(sqlc.M(model.OwSymbol{}).Eq("coin", symbol).Eq("state", 1), &find); err != nil {
		return nil, err
	}
	if find.Id == 0 {
		return nil, errors.New("symbol notfound: " + symbol)
	}
	return &find, nil
}

func (o *OpenWScanner) LoadConfig(symbol string) (config.Configer, error) {
	consulx, err := new(consul.ConsulManager).Client()
	if err != nil {
		panic(err)
	}
	result, err := consulx.GetKV(util.AddStr("coin/", symbol, ".ini"))
	if err != nil {
		log.Error(symbol, " load error: ", err.Error())
		return nil, err
	}
	c, err := config.NewConfigData("ini", result)
	if err != nil {
		log.Error("load ini config err: ", err.Error())
		return nil, err
	}
	o.DbPath = c.String("dataDir")
	o.DbName = symbol + ".db"
	return c, nil
}

func getAccountIDByAddress(address, symbol string) (string, error) {
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		log2.Error(util.AddStr("[", symbol, "]通过地址[", address, "]获取redis失败: ", err), 0)
		return "", nil
	}
	obj := major.CacheValue{}
	if _, err := client.Get(util.AddStr(symbol, address), &obj); err != nil {
		log2.Error(util.AddStr("[", symbol, "]通过地址[", address, "]读取账号ID失败: ", err), 0)
		return "", nil
	}
	if len(obj.V) == 0 {
		if symbol == model.ETH {
			if _, err := client.Get(util.AddStr(model.TRUE, address), &obj); err != nil {
				log2.Error(util.AddStr("[", model.TRUE, "]通过地址[", address, "]读取账号ID失败: ", err), 0)
				return "", nil
			}
		}
		if len(obj.V) == 0 {
			return "", nil
		}
	}
	return obj.V, nil
}

func getAccountIDByAlias(alias, symbol string) (string, error) {
	client, err := new(cache.RedisManager).Client()
	if err != nil {
		log2.Error(util.AddStr("[", symbol, "]通过别名[", alias, "]获取redis失败: ", err), 0)
		return "", nil
	}
	obj := major.CacheValue{}
	if _, err := client.Get(util.AddStr(symbol, alias), &obj); err != nil {
		log2.Error(util.AddStr("[", symbol, "]通过别名[", alias, "]读取账号ID失败: ", err), 0)
		return "", nil
	}
	if len(obj.V) == 0 {
		return "", nil
	}
	return obj.V, nil
}

// GetAssetsController 获取资产控制器 -
func GetAssetsManager(symbol string) (openwallet.AssetsAdapter, error) {
	adapter := assets.GetAssets(symbol)
	if adapter == nil {
		return nil, fmt.Errorf("assets: %s is not support", symbol)
	}

	manager, ok := adapter.(openwallet.AssetsAdapter)
	if !ok {
		return nil, fmt.Errorf("assets: %s is not support", symbol)
	}
	return manager, nil
}
