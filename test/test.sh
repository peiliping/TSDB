lua TSDB.lua fix BTC_KL_5M 0 0

cat data/kline.csv | lua TSDB.lua write BTC_KL_5M 

lua TSDB.lua rollup BTC_KL_5M BTC_KL_15M 1725120000 1784005200

lua TSDB.lua parallel BTC_KL_5M BTC_LN_5M 1725120000 1784005200 288
