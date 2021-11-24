# Google File System

A basic and simple implementation of Google File System in Python

Paper Link: https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf

## How to run?

* Master
	* python3 primary.py
	* python3 secondary.py
* ChunkServer
	* python3 communication.py 5001
	* python3 communication.py 5002
	* python3 communication.py 5003
* Client
	* python3 client.py

## Client Commands

* Read File
	* read filename
* Write File
	* write filename
* Append File
	* append tofile fromfile