# VCF_crawler

The program VCF_cralwer.py is a python3 program. The purpose of the app is to download the lastest version of the NCBI clinvar file (vcf.gz), and create two json files, nodes.json and links.json, based on the vcf file. 

The app gets the number of processes (e.g., 4), and an FTP url as input. For instance: 


```python
python VCF_crawler.py 4 ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/weekly/clinvar.vcf.gz
```

The default value for the ftp url is the same as the above example, and for the number of processes is **1** (single process). Therefore, if your intention is the above vcf file, you will only need to specify the number of processes in the command, such as the following:


```python
python VCF_crawler.py 4
```

The craweler does the following tasks in order:

    1- Download the vcf.gz file.
    2- Unzip the vcf file.
    3- Convert the vcf file to csv (i.e., remove the vcf headers, save the data lines in 8 columns: [CHROM,POS,ID,REF,ALT,QUAL,FILTER,INFO])
    4- Extract the necessary information from the csv file and create the related json files (node.json and links.json)

The last step, i.e., processing the csv file and creating json files, can be done either in single process or multi-process form. If the vcf file is large, such as the above clinvar, multi-threading or multi-processing makes the crawling much faster. Here is the result of the multi-processing crawling which I tested in my own computer (single processor with 4 cores):        

### Test I: 
n_process = 2


```python
The file: clinvar.vcf.gz, downloaded in 8s
The file: clinvar.vcf.gz, extracted to clinvar.vcf, in 6s
The file: clinvar.csv produced in 13s
```


```python
The json files: nodes.json and links.json created from clinvar.csv in 11527s
```

### Test II: 
n_process = 4


```python
The json files: nodes.json and links.json created from clinvar.csv in 917s
```

It indicates that the crawler when n_process=4, works 12 times faster than when we have n_process=2 (i.e., 15min vs. 3h15m)

Created json files, nodes.json and links.json, for the latest version of the clinvar file (May 2021) are 165MB and 32MB, respectively. Thus, I included subsets of these files in this repo as output samples.

## Multi-processing design

For having an efficient parallellism, I considered two types of processes: 

    1- Workers, or line processors who extract information from a row in the csv file, construct the related json nodes and links object, and put in a queue.  
    
    2- Listener, or json writer who gets an object from the queue and writes in the related json file. Thus, only one process does the writing job.
       
In this design, the workers do their jobs in parrallel. On the other hand, the listener or file writer does its job in parallel to the workers. We do not need to define locks for the json files, as only one process has access to those.

Another adopted tactic is in appending a json object to a json file which is in the form of json array. A typical procedure in python is comprised of reading the json array to memory, append the object, and then dump the array to the json file. However, this procedure for large files is not practical. Therefore, I used an adhoc text processing solution to append a json object at the end of json array in the file.  

## Dockerfile

Basically, having python3 is enough to run the script, as I did not use any non-built-in packages or modules. Nevertheless, I also wrote a dockerfile. However, when I tested the dockerfile, i.e., build the image and ran the container, I noticed that it takes hours to make the json files in multiprocess mode on my computer. I could not figure out why is that. It seems that the container only uses one cpu (core) to run the program. 