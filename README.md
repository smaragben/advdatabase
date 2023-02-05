# Εξαμηνιαία Εργασία μαθήματος Προχωρημένα Θέματα Βάσεων Δεδομένων
Σμαραγδή Μπενέτου(el18048) - Ομάδα 54

Οδηγίες εγκατάστασης περιβάλλοντος:

1. Και στα δύο μηχανήματα αλλάζουμε το αρχείο /etc/hosts προσθέτοντας:
```
192.168.0.1     namenode
192.168.0.2     datanode1
```
2. Αλλάζουμε τα hostname στα δύο μηχανήματα θέτοντας του slave datanode1 και του master namenode.
```
hostnamectl set-hostname new-hostname
```
3. Ακολουθήθηκαν οι οδηγοί:  
Για εγκατάσταση του Hadoop  
https://sparkbyexamples.com/hadoop/apache-hadoop-installation/  
Για εγκατάσταση του Yarn:  
https://sparkbyexamples.com/hadoop/yarn-setup-and-run-map-reduce-program/  
Και εγκατάσταση του Spark:  
https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/  
Τα αντίστοιχα configuration αρχεία που αλλάχθηκαν για το Spark και το Hadoop μπορούν να βρεθούν στο repository αυτό στους φακέλους Hadoop_config και Spark_config.

4.Για να εκκινήσουν όλοι οι hadoop daemons εκτελούμε:  
```
start-dfs.sh
```

5.Για να ξεκινήσουμε το yarn εκτελούμε:  
```
start-yarn.sh
```

6.Για να τρέξουμε ένα από τα script στο cluster χρησιμοποιούμε την εντολή:  
Για 2 workers  
```
spark-submit --deploy-mode cluster --num-executors 2 --executor-cores 4 <python_script_path>
```  
Για 1 worker  
```  
spark-submit --deploy-mode cluster --num-executors 2 --executor-cores 4 <python_script_path>
```

7. Για να δούμε το output του παραπάνω script μετά την εκτέλεσή του γνωρίζοντας το appId από τις λεπτομέρειες της εκτέλεσης:
```
yarn logs -applicationId <appId> -log_files stdout
```

8.Για να πάρουμε ένα αρχείο εξόδου από το hdfs στο τοπικό directory εκτελούμε:
```
hdfs dfs -get output/<filename>
```

9.Για να πάρουμε ένα αρχείο από το τοπικό directory στον προσωπικό μας υπολογιστή εκτελούμε:
```
scp -r user@snf-34495.ok-kno.grnetcloud.net:/home/user/<filepath>  ~/
```
