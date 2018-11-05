
.PHONY: gacspp
gacspp:
	g++ -O3 -march=native -std=c++17 -Wall $(wildcard *.cpp) -o gacspp.out -lmgl-qt5 -lmgl
