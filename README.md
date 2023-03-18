# About the project

This is an implementation of RAFT consensus algorithm. To learn more about RAFT visit this website: [thesecretlivesofdata](https://thesecretlivesofdata.com/raft/)

# How to run

## Requirements

* ```python3.8```

## Build gRPC

Install requirements:

```bash
pip install -r requirements.txt
```

Then run:

```bash
python3 -m grpc_tools.protoc raft.proto --proto_path=. --python_out=. --grpc_python_out=.
```

## Run Server

After setting the id, ip address, and port for each server in `config.conf` file, run:

> Don't add extra new lines to the config file

```bash
python server.py <server_id>
```

## Run Client

```bash
python client.py
```