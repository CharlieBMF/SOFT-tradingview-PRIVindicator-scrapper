import re

with open('symbols_html_stock_5BMarketCap.txt', 'r') as file:
    content = file.read()

symbols_raw_list = re.findall(r'data-rowkey="([^"]+)"', content)
print(symbols_raw_list)

with open('stock_symbols_raw_list.txt', 'w') as output_file:
    for symbol in symbols_raw_list:
        output_file.write(symbol + '\n')