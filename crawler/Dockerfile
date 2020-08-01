FROM selenium/standalone-firefox:latest

ARG START_DATE
ARG END_DATE

WORKDIR /home/seluser

# crawler files
ADD https://raw.githubusercontent.com/burakyilmaz321/fonapi/master/crawler/crawler.py .
ADD https://raw.githubusercontent.com/burakyilmaz321/fonapi/master/crawler/requirements.txt .

# Install apt and pip requirements
RUN sudo apt-get update \
    && sudo apt-get install -y --no-install-recommends \
        python3-pip \
    && python3 -m pip install pip setuptools --upgrade \
    # crawler requirements
    && sudo python3 -m pip install -r requirements.txt

ENTRYPOINT [ "python3", "crawler.py" ]
