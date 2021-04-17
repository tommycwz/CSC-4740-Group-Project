Make sure you have the test file in hdfs,
hdfs dfs -put CleanLichessDataset.txt chessFile

To run it,
hadoop jar chesstest.jar tigerGambit.ChessAnalysis chessFile chessout
