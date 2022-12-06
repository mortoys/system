# from futu import *

# quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)  # 创建行情对象
# print(quote_ctx.get_market_snapshot('HK.00700'))  # 获取港股 HK.00700 的快照数据
# quote_ctx.close() # 关闭对象，防止连接条数用尽


from futu import *
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

ret_sub, err_message = quote_ctx.subscribe(['HK.00700'], [SubType.QUOTE], subscribe_push=False)
# 先订阅 K 线类型。订阅成功后 FutuOpenD 将持续收到服务器的推送，False 代表暂时不需要推送给脚本

if ret_sub == RET_OK:  # 订阅成功
    ret, data = quote_ctx.get_stock_quote(['HK.00700'])  # 获取订阅股票报价的实时数据
    if ret == RET_OK:
        print(data)
        print(data['code'][0])   # 取第一条的股票代码
        print(data['code'].values.tolist())   # 转为 list
    else:
        print('error:', data)
else:
    print('subscription failed', err_message)

quote_ctx.close()  # 关闭当条连接，FutuOpenD 会在1分钟后自动取消相应股票相应类型的订阅