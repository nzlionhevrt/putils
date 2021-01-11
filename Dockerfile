FROM python:3

ADD my_script.py /

RUN pip install schedule

CMD [ "python", "./uploader.py" ]
