# pull python3 base image
FROM python:3.5-onbuild

RUN mkdir /usr/src/workshop
RUN mkdir /usr/src/workshop/tools
RUN mkdir /usr/src/workshop/logs

# copy all into app folder
COPY . /usr/src/workshop/tools

WORKDIR /usr/src/workshop

ENV PYTHONPATH "${PYTONPATH}:/usr/src/workshop"

# run server  
RUN ["./tools/build.py"]
