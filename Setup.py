
- Host file entry
    C:\Windows\System32\drivers\etc\hosts
    127.0.0.1       sandbox-hdp.hortonworks.com

- Change Python to 3.6
	○ Ambari -> Spark2 -> Configs -> Advanced spark2-env -> content
	○ Add the two lines
	
- Data
Remove double quotes
		
- Copy data files
mkdir data on CentOS
scp -P 2222 D:\Prasanna\Nootus\Repos\BigDataPractice\airports.csv maria_dev@localhost:/home/maria_dev/data
scp -P 2222 D:\Prasanna\Nootus\Repos\BigDataPractice\airport-frequencies.csv maria_dev@localhost:/home/maria_dev/data

# first create a HDFS file
hadoop fs -mkdir data
hadoop fs -copyFromLocal "/home/maria_dev/data/airports.csv" "/tmp/data"
hadoop fs -copyFromLocal "/home/maria_dev/data/airport-frequencies.csv" "/tmp/data"



