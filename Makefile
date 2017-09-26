INCLUDES = -I/usr/include/boost -I/usr/include/mysql -I./lib
LINKS = -L/usr/lib/x86_64-linux-gnu -lmysqlclient -lpthread -lz -lm -lrt -ldl


default: obj/visitor.o obj/circlemaker.o obj/main.o
	@g++ -Wall -g obj/visitor.o obj/circlemaker.o obj/main.o -o makecircles $(LINKS)
	@echo Linking... 

obj/main.o: circlemaker.h main.cpp 
	@g++ -Wall -std=c++11 $(INCLUDES) -c main.cpp -o obj/main.o 
	@echo Making Main

obj/circlemaker.o: circlemaker.cpp circlemaker.h 
	@g++ -Wall $(INCLUDES) -c circlemaker.cpp -o obj/circlemaker.o 
	@echo Making CircleMaker

obj/visitor.o: visitor.cpp visitor.h
	@g++ -Wall $(INCLUDES) -c visitor.cpp -o obj/visitor.o
	@echo Making Visitor

clean:
	@echo Cleaning...
	@rm obj/main.o
	@rm obj/circlemaker.o
	@rm obj/visitor.o
	@rm makecircles
