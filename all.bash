#!/bin/bash
cd ot; make clean; make install; cd ..
cd store; make clean; make install; cd ..
cd grapher; make clean; make install; cd ..
cd federation; make clean; make install; cd ..
cd transformer; make clean; make install; cd ..
cd api; make clean; make install; cd ..
cd samples/gocurses; make clean; make install; cd ../..
cd samples/p2p_editor; make clean; make; cd ../..
