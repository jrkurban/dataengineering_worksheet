### Q-1: 
- Download and put `https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv` dataset in hdfs `/user/train/hdfs_odev` directory.

Answer :wget https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv
        hdfs dfs -mkdir /user/train/hdfs_odev
		hdfs dfs -put ~/Wine.csv /user/train/hdfs_odev

### Q-2:
- Copy this hdfs file `/user/train/hdfs_odev/Wine.csv` to `/tmp/hdfs_odev` hdfs directory.

Answer : hdfs dfs -cp /user/train/hdfs_odev2/Wine.csv /tmp/hdfs_odev

### Q-3:
- Delete `/tmp/hdfs_odev` directory with skipping the trash. 

Answer :  hdfs dfs -rm -skipTrash /tmp/hdfs_odev

### Q-4:
-  Explore `/user/train/hdfs_odev/Wine.csv` file from web hdfs. 

Answer : http://localhost:9870/explorer.html#/