del /Q out
rmdir out
:: ../bin/hadoop jar wc.jar WordCount ./in ./out
:: ../bin/hadoop jar wc.jar ./in ./out
:: ../bin/hadoop jar wc.jar ./in2 ./out
../bin/hadoop jar wc.jar ./in100 ./out