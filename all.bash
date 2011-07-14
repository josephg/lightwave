#!/bin/bash
cd ot; make clean; make install; cd ..
cd store; make clean; make install; cd ..
cd grapher; make clean; make install; cd ..
cd federation; make clean; make install; cd ..
cd transformer; make clean; make install; cd ..
