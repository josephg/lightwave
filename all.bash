#!/bin/bash
cd ot; make clean; make install; cd ..
cd store; make clean; make install; cd ..
cd indexer; make clean; make install; cd ..
cd federation; make clean; make install; cd ..
