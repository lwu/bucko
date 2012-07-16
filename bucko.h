#ifndef BUCKO_H
#define BUCKO_H

#include <vector>
#include <ostream>
#include <fstream>

typedef int DataType;
typedef std::vector<DataType> Tuple;
typedef std::vector<Tuple> TupleVector;
typedef TupleVector::iterator TViterator;

// Count structure for g_outputRec
struct Count
{
	Count(int cnt = 0, bool allPred = false) {
		isAll = allPred;
		count = cnt;
	}

	int  count;
	bool isAll;
};

std::ostream& operator<<(std::ostream& os, const Count& c)
{
	if (c.isAll) {
		os << "*";
	} else {
		os << c.count;
	}

	return os;
}

// Directs debugging output to console and/or file
struct DebugStream
{
	DebugStream()
	{
		enableConsoleOutput = true;
		enableFileOutput = false;
	}	

	void dumpToFile() {
		enableFileOutput = true;
		fileOut.open("debug.txt");	
	}

	void endl() {
		// Handle endl calls differently
		if (enableConsoleOutput) std::cout << std::endl;
		if (enableFileOutput) fileOut << std::endl;
	}

	bool enableConsoleOutput;
	bool enableFileOutput;
	std::ofstream fileOut;
};

// Overload insertion operator from debugging stream
template <class T>
DebugStream& operator<<(DebugStream& os, const T& obj) {
	if (os.enableConsoleOutput) std::cout << obj;
	if (os.enableFileOutput) os.fileOut << obj;

	return os;
}


#endif