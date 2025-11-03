# TSDB
A Simple TimeSeriesDatabase For CMD


## Usage

touch /root/tsdb/data/BTCUSDT_5MIN.bin
touch /root/tsdb/data/BTCUSDT_4H.bin

lua TSClient.lua stat BTCUSDT_5MIN
lua TSClient.lua stat BTCUSDT_4H

lua TSClient.lua write BTCUSDT_5MIN 1725121200 58958.00 58968.20 58927.80 58945.00 187.449 11048014.52910 58.803 3465929.45330 4160 0.342 47.93

lua TSClient.lua read BTCUSDT_5MIN 1725121200 1735121200

cat /data/btcusdt_5min.csv | lua TSClient.lua write BTCUSDT_5MIN

lua TSClient.lua rollup BTCUSDT_5MIN BTCUSDT_4H 1725121200 1761897600