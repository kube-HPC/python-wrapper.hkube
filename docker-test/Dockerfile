FROM python:3.13.0b3
ADD requirements.txt /hkube/algorithm-runner/requirements.txt
WORKDIR /hkube/algorithm-runner
RUN pip3 install -r requirements.txt
COPY . /hkube/algorithm-runner
CMD ["python3", "-u", "main.py"]