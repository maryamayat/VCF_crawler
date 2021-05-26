FROM python:3
WORKDIR /app
COPY . /app
CMD ["python","VCF_crawler.py", "4"]