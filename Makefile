
.PHONY: gacspp
gacspp:
	g++ -g -pg -Wall $(wildcard *.cpp) -o gacspp.out -lmgl-qt5 -lmgl
