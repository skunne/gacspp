
.PHONY: gacspp
gacspp:
	g++ -Wall $(wildcard *.cpp) -o gacspp.out -lmgl-qt5 -lmgl
