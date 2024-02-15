FROM python

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install pandas
CMD ["python","./main.py"]
