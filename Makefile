
.PHONY: gacspp
gacspp:
	g++ -O3 -march=native -Wall $(wildcard *.cpp) -o gacspp.out -lmgl-qt5 -lmgl
