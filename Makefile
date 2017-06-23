SRC := ./pio.cpp 
OBJS := $(SRC:%.cpp=%.o)

CXXFLAGS := $(CXXFLAGS) -std=c++11 -O3 -D_FILE_OFFSET_BITS=64
LDFLAGS := $(LDFLAGS) -lpthread 

./%.o: ./%.cpp
	$(MPICXX) -c $(CXXFLAGS) $< -o $@

pio: $(OBJS)
	$(MPICXX)  $(OBJS) -o "$@" 	

clean:
	rm -fr ./*.o ./pio	
 
	