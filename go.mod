module github.com/nbit99/open_scanner

go 1.12

require (
	github.com/astaxie/beego v1.12.0
	github.com/blocktree/go-openw-server/open_base v1.0.0
	github.com/blocktree/openwallet/v2 v2.0.9
	github.com/godaddy-x/jorm v1.0.60
	github.com/shopspring/decimal v0.0.0-20200105231215-408a2507e114
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
)

replace github.com/blocktree/go-openw-server/open_base => ../open_base
