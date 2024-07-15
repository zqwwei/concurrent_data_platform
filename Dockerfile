FROM python:3.8-slim

# set CWD
WORKDIR /app

# copy needed files
COPY . /app/

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install gunicorn

# RUN apt-get update && apt-get install -y curl

# create data folder
RUN mkdir -p /app/data

# expose two ports
EXPOSE 9527 7259

# execute program
CMD ["gunicorn", "-b", "0.0.0.0:5000", "main:app"]
