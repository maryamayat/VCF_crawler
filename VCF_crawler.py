import sys
from urllib.parse import urlparse
from ftplib import FTP
from datetime import datetime
import gzip
import shutil
import os
import json
import multiprocessing as mp

class VCF_crawler:

    def __init__(self, url):

        self.url = {}
        self.vcf_fname = {}
        self.json_fname = {}

        url_parse_result = urlparse(url)

        self.url["serverloc"] = url_parse_result.netloc

        start_pos = url_parse_result.path.rfind("/")
        end_pos = len(url_parse_result.path)
        self.vcf_fname["zipped"] = url_parse_result.path[start_pos + 1:end_pos]

        self.url["path"] = url_parse_result.path[:start_pos + 1]

        self.vcf_fname["unzipped"] = os.path.splitext(self.vcf_fname["zipped"])[0]

        self.vcf_fname["csv"] = os.path.splitext(self.vcf_fname["unzipped"])[0]+".csv"

        self.json_fname["nodes"] = "nodes.json"
        self.json_fname["links"] = "links.json"


    # Download the file from the ftp server
    #
    def download_vcf(self):

        start = datetime.now()

        ftp = FTP(self.url["serverloc"])  # FTP("ftp.ncbi.nlm.nih.gov")
        ftp.login("", "")
        ftp.cwd(self.url["path"])
        ftp.retrbinary("RETR " + self.vcf_fname["zipped"], open(self.vcf_fname["zipped"], 'wb').write)
        # ftp.quit()
        ftp.close()

        end = datetime.now()
        diff = end - start
        print(f'The file: {self.vcf_fname["zipped"]}, downloaded in {str(diff.seconds)}s')


    # Unzip the zipped clinvar file and update the unzipped filename
    #
    def unzip_vcf(self):

        start = datetime.now()

        with gzip.open(self.vcf_fname["zipped"], 'rb') as f_in:
            with open(self.vcf_fname["unzipped"], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        end = datetime.now()
        diff = end - start
        print(f'The file: {self.vcf_fname["zipped"]}, extracted to {self.vcf_fname["unzipped"]}, in {str(diff.seconds)}s')



    # Convert the vcf file to tabular format (csv file)
    #
    def vcf2csv(self):

        start = datetime.now()

        out_header = ["#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER","INFO"]
        with open(self.vcf_fname["unzipped"], "r") as inVCF, open(self.vcf_fname["csv"], "w") as outCSV:

            for line in inVCF:

                if line.startswith("##"):
                    # skip the meta-information
                    continue

                elif line == "\n" or line == "\r\n":
                    # skip the empty line (e.g., the empty line at the end of the file)
                    continue

                elif line.startswith("#CHROM"):
                    outCSV.write("\t".join(out_header)+"\n")

                else:
                    l  = line.split("\t",8)

                    if len(l) > 8:
                        # There are columns after INFO, and we do not need them
                        outCSV.write("\t".join(l)[:8]+"\n")

                    else:
                        # There are not any other column after INFO
                        # Drop "\r" if there is at the second last char of the line and write the line in outfile
                        l = line.split("\r")
                        outCSV.write("".join(l))

        end = datetime.now()
        diff = end - start
        print(f'The file: {self.vcf_fname["csv"]} produced in {str(diff.seconds)}s')


    # Helper function in process_line
    # (single process version)
    #
    def write_json(self, new_data, filename):

        with open(filename, 'r+') as f:
            f.seek(0,2)
            position = f.tell()-1
            f.seek(position)
            f.write("{},]".format(json.dumps(new_data, indent=4)))

    # Extract information from a csv line to one json_node and one json_link objects and write in the related json files
    # (single process version)
    #
    def process_line(self, line):

        row = line.strip()
        row = row.split("\t")

        jnode = json_node()
        jnode.update(row)

        self.write_json(jnode.get_node(), self.json_fname["nodes"])

        jlink = json_link()
        if jlink.update(row) != -1:
            self.write_json(jlink.get_link(), self.json_fname["links"])


    # Convert vcf(csv format) to json files (nodes and links)
    # (single process version)
    #
    def csv2json(self):

        start = datetime.now()

        with open(self.json_fname["nodes"], 'w') as outJNODE:
            json.dump([], outJNODE)

        with open(self.json_fname["links"], 'w') as outJLINK:
            json.dump([], outJLINK)

        with open(self.vcf_fname["csv"], 'r') as inCSV:

            # Skip the header
            next(inCSV)

            # Process the other lines:
            #
            for line in inCSV:
                self.process_line(line)

        # Remove the last comma from the json array in the files
        with open(self.json_fname["links"], 'r+') as outJNODE:
            outJNODE.seek(0,2)
            position = outJNODE.tell()-2
            outJNODE.seek(position)
            outJNODE.write(" ]")
        with open(self.json_fname["links"], 'r+') as outJLINK:
            outJLINK.seek(0,2)
            position = outJLINK.tell()-2
            outJLINK.seek(position)
            outJLINK.write(" ]")

        end = datetime.now()
        diff = end - start
        print(f'The json files: {self.json_fname["nodes"]} and {self.json_fname["links"]} created from {self.vcf_fname["csv"]} in {str(diff.seconds)}s')


    # Lisener process
    #
    def json_writer(self, q):
        with open(self.json_fname["nodes"], 'r+') as f1, open(self.json_fname["links"], 'r+') as f2:
            while (1):
                new_data = q.get()
                if new_data == 'kill':
                    break
                if new_data[0] == 'node':
                    f1.seek(0, 2)
                    position = f1.tell() - 1
                    f1.seek(position)
                    f1.write("{},]".format(json.dumps(new_data[1], indent=4)))
                elif new_data[0] == 'link':
                    f2.seek(0, 2)
                    position = f2.tell() - 1
                    f2.seek(position)
                    f2.write("{},]".format(json.dumps(new_data[1], indent=4)))

            # Remove the last comma from the json array in the files
            f1.seek(0, 2)
            position = f1.tell() - 2
            f1.seek(position)
            f1.write(" ]")
            f2.seek(0, 2)
            position = f2.tell() - 2
            f2.seek(position)
            f2.write(" ]")

    # Worker process
    #
    def line_processor(self, line, q):

        row = line.strip()
        row = row.split("\t")

        jnode = json_node()
        jnode.update(row)

        q.put(["node", jnode.get_node()])

        jlink = json_link()
        if jlink.update(row) != -1:
            q.put(["link", jlink.get_link()])


    # Convert vcf(csv format) to json files (nodes and links) - multiprocessing
    # We assign the writing files task to one single process (i.e., json_writer),
    # with a Queues as input that is fed by the other processes (i.e., line_processors).
    #
    def csv2json_mp(self, n_process=2):

        start = datetime.now()

        with open(self.json_fname["nodes"], 'w') as outJNODE:
            json.dump([], outJNODE)

        with open(self.json_fname["links"], 'w') as outJLINK:
            json.dump([], outJLINK)

        manager = mp.Manager()
        q = manager.Queue()
        pool = mp.Pool(processes = n_process)

        # First, we put the json_writer to work
        write_job = pool.apply_async(self.json_writer, (q,))

        # Now, we assign works to line_processors
        jobs = []
        with open(self.vcf_fname["csv"], 'r') as inCSV:

            # Skip the header
            next(inCSV)

            for line in inCSV:
                job = pool.apply_async(self.line_processor, (line, q,))
                jobs.append(job)

        # Wait for all the line_processors to finish
        for job in jobs:
            job.get()

        # Kill the json_writer
        q.put('kill')

        # Clean up
        pool.close()
        pool.join()

        end = datetime.now()
        diff = end - start
        print(f'The json files: {self.json_fname["nodes"]} and {self.json_fname["links"]} created from {self.vcf_fname["csv"]} in {str(diff.seconds)}s')

#
# class VCF_crawler: end of definition

# Helper function used in the classes: json_node and json_link
#
def get_INFOkey_value(sub, s):

    if sub in s:
        start_pos = s.find("=")+1
        return s[start_pos:]
    return None


class json_node:

    def __init__(self):

        self.node = {
            "CHROM": None,
            "POS": None,
            "ID": None,
            "REF": None,
            "ALT":None,
            "AF_ESP": None,
            "AF_EXAC": None,
            "AF_TGP": None,
            "ALLELEID": None
        }


    # Update the values in the node having data which is a csv line as a list
    #
    def update(self, data):

        self.node["CHROM"] = data[0]
        self.node["POS"] = int(data[1])
        self.node["ID"] = data[2]
        self.node["REF"] = data[3]
        self.node["ALT"] = data[4]

        # Process INFO part in data (last item, 8th item) to extract value for the four remaining keys (AF_ESP, etc.)
        #
        INFO_list = data[-1].split(";")
        remaining = list(self.node.keys())[5:]
        for key in remaining:
            for item in INFO_list:
                value = get_INFOkey_value(key, item)
                if value:
                    if key==remaining[3]:  # i.e., key=="ALLELEID":
                        self.node[key] = int(value)
                    else:
                        self.node[key] = float(value)
                    break
        return 1


    def get_node(self):
        return self.node

#
# class json_node: end of definition


class json_link:

    def __init__(self):

        self.link = {
            "_from": None,
            "_to": None
        }


    # Update the values in the node having data which is a csv line as a list
    #
    def update(self, data):

        # Process INFO part in data (last item, 8th item) to extract RS value for the remaining key ("_to")
        # If there is RS, update "_from" value for the link, and return 1
        # Otherwise, return -1, as there is no link info in this data
        #
        INFO_list = data[-1].split(";")
        key = "RS"
        for item in INFO_list:
            value = get_INFOkey_value(key, item)
            if value:
                self.link["_to"] = value
                self.link["_from"] = data[2]
                return 1
        return -1


    def get_link(self):
        return self.link

#
# class json_link: end of definition


# Return the command line arguments (if no argument, return defaults)
# First argument: ftp-url
# Second argument; number of processors
#
def get_args(name='VCF_crawler.py',
             first=1,
             second='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/weekly/clinvar.vcf.gz'):
    return int(first), second

def main():

    # Get the number of processes/threads and the ftp url as arguments, e.g.,
    # python VCF_crawler.py 4 ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/weekly/clinvar.vcf.gz
    n_process, url = get_args(*sys.argv)

    # Create the crawler, and
    # 1- Download the vcf.gz file
    # 2- Unzip the file
    # 3- Convert the vcf to csv file
    # 4- Create two json files from the csv file
    crawler = VCF_crawler(url)
    crawler.download_vcf()
    crawler.unzip_vcf()
    crawler.vcf2csv()
    if n_process >= 2:
        # run multi process version
        crawler.csv2json_mp(n_process)
    else:
        # run single process version
        crawler.csv2json()


if __name__ == "__main__":
    main()