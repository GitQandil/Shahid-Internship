FROM python
WORKDIR ~/Desktop/last
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python","./main.py"]a