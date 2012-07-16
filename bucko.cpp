// Leslie Wu
//
// A C++ implementation of BUC--bottom-up computation of sparse and iceberg CUBEs

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cassert>
#include <functional>
#include <string>
#include <map>

#include "bucko.h"

using namespace std;

// Globals
int g_minsup;                   // minimum support threshold
vector<Count> g_outputRec;      // current output record
Count ALL(0, true);             // dummy variable to represent "ALL"
vector<int> g_dimensionIndices; // ordering of dimension indices

// Stream output
DebugStream debug;
ofstream outfile1;            // output file I
ofstream outfile2;            // output file II
map<string, int> cuboidCount; // cuboid count based on ID

// Print command-line usage information
void usage()
{
	cout << "usage:\n\tbucko [input datafile] [minsup]\n" << endl;
}

// Return true iff no non-whitespace characters between here and EOL
bool iseol(istream& is)
{
	// Eat whitespace
	while (!is.eof() && isspace(is.peek())) {		
		char ch = is.get(); // Eat

		if (ch == '\n') return true;
	}
	
	return false;
}

// Read input tuples from file [filename] into tuple data structures
bool read_data(const char* filename, 
			   TupleVector& input, 
			   Tuple& cardinalities)
{
	// Open file
	ifstream infile(filename);

	if (!infile) {
		return false;
	}

	// Clear data
	input.clear();
	cardinalities.clear();

	// Read data header:
	int tuple_count;	
	infile >> tuple_count;

	debug << "Data header  : " << tuple_count << " tuples."; debug.endl();
	debug << "Cardinalities: ";

	while (!iseol(infile)) {
		int c;
        infile >> c;

		debug << c << ", ";

        cardinalities.push_back(c);
	}
	
	debug.endl();
	int dim = (int)cardinalities.size();
	debug << "Dimensions   : " << dim; debug.endl();

	input.reserve(tuple_count);

	// Read tuples
	for (int i=0; i < tuple_count; i++) {
        Tuple t;

		assert(!infile.eof());		

		for (int j=0; j < dim; j++) {
			DataType data;
			infile >> data;
			t.push_back(data);

			debug << data << ", ";
		}

		debug.endl();

		input.push_back(t);
	}

	return true;
}

struct tuple_less_than
{
	tuple_less_than(int d) {
		dim = d;
	}

	bool operator()(const Tuple& lhs, const Tuple& rhs) {
		return (lhs[dim] < rhs[dim]);
	}

	int dim;
};

bool pair_greater(const pair<int,int>& lhs, const pair<int,int>& rhs)
{
	return (lhs.first > rhs.first);
}

void order_dimensions(Tuple& cardinalities)
{
	// Copy cardinalities into augmented array, sort, dump sorted indices
	// to global array for dimension ordering

	int i;

	typedef pair<int,int> IntPair;
	vector<IntPair> cardinalityPairs;

	for (i=0; i < (int)cardinalities.size(); i++) {
		cardinalityPairs.push_back( make_pair(cardinalities[i], i) );
		debug << cardinalityPairs[i].first << ", " << cardinalityPairs[i].second; debug.endl();
	}
	
	cout << endl;

	stable_sort(cardinalityPairs.begin(), cardinalityPairs.end(), pair_greater);
	g_dimensionIndices.clear();

	for (i=0; i < (int)cardinalities.size(); i++) {
		g_dimensionIndices.push_back(cardinalityPairs[i].second);

		debug << cardinalityPairs[i].first << ", " << cardinalityPairs[i].second; debug.endl();
	}

}

// Partitions a tuple vector [begin...end] along dimension [dim],
// which has cardinality [cardinality]
void partition_input(TViterator begin, TViterator end, 
					 int dim, int cardinality, 
					 vector<int>& dataCount)
{
	assert(dataCount.size() == cardinality+1);

	stable_sort(begin, end, tuple_less_than(dim));
	
	for (TViterator it = begin; it != end; ++it) {
		Tuple& t = *it;

		DataType data = t[dim];
		assert(data >= 0);
		assert(data < cardinality+1);

        dataCount[data]++;
	}
}

void write_outputRec(DataType aggregate, ostream& os)
{
	os << "(";

	copy(g_outputRec.begin(), g_outputRec.end(),
		ostream_iterator<Count>(os, " "));

	os << ") " << aggregate << endl;
}

void count_cuboid()
{
	// Name cuboid based on dimension ('a' first, 'b' second, etc.)
	string cuboidId;

	for (int i=0; i < (int)g_outputRec.size(); i++) {
		Count c = g_outputRec[i];
		if (!c.isAll) {
			char dimChar = 'a' + i;

			cuboidId += dimChar;
		}
	}

	// Add one to cuboid count
	if (!cuboidId.empty()) {
		cuboidCount[cuboidId]++;
	}
}

// Write cuboid counts to an output stream
void write_cuboid_counts(ostream& os)
{
	map<string,int>::iterator it, end;
	it = cuboidCount.begin();
	end = cuboidCount.end();

	for ( ; it != end; ++it) {
		os << it->first << ":" << it->second << " " << endl;
	}
}

// Implements Kevin Beyer and Raghu Ramakrishnan's "BUC"
void bottom_up_cube(TupleVector& input, Tuple& cardinalities, 				
					int dim,
					TViterator begin, TViterator end)
{
	int numDims = (int)cardinalities.size();	

	assert(dim >= 0);
	assert(dim <= numDims);
	
	// Aggregate input

	DataType aggregate_size = (DataType)distance(begin, end);
	// write_outputRec(aggregate_size, cout);	
	write_outputRec(aggregate_size, outfile1);	
	count_cuboid();

	for (int dIndex = dim; dIndex < numDims; dIndex++) {
		int d = g_dimensionIndices[dIndex];

		int C = cardinalities[d];

		vector<int> dataCount(C+1);

		partition_input(begin, end, d, C, dataCount);

		TViterator k = begin;

		for (int i=0; i <= C; i++) {
			int c = dataCount[i];

			if (c >= g_minsup) {
				g_outputRec[d] = (*k)[d];
				bottom_up_cube(input, cardinalities, dIndex+1,
					k, k + c);
			}

			k += c;
		}

		g_outputRec[d] = ALL;
	}	
}

int main(int argc, char* argv[])
{
	cout << "\nBUC++ by Andrew Wu <awu@uiuc.edu>\n" << endl;

	// Check arguments
	if (argc < 3) {
		usage();
		return 1;
	}

	// Get arguments
	char* filename = argv[1];
	char* minsupStr = argv[2];

	// Read data
	debug << "Reading input: [" << filename << "]";	debug.endl();


	// Turn on debugging?
	bool debugToConsole = (argc > 3);	
	if (debugToConsole) {
		debug.enableConsoleOutput = true;
	} else {
		debug.enableConsoleOutput = false;
		debug.dumpToFile();
	}

	TupleVector input;
	Tuple cardinalities;
	bool rv = read_data(filename, input, cardinalities);

	if (!rv) {
		cerr << "Couldn't read input data! Aborting..." << endl;
		return 1;
	}

	// Figure out dimension ordering
	order_dimensions(cardinalities);

	// Open output files
	outfile1.open("out.1");
	outfile2.open("out.2");

	// Get "minsup" from command line
	int minsup = atoi(minsupStr);
	outfile1 << "minsup: " << minsup << endl;

	// Set options
	g_minsup = minsup;

	int numDims = (int)cardinalities.size();
	g_outputRec = vector<Count>(numDims, ALL);

	cout << "Running bottom up computation of data cube... ";

	// Perform "BUC"
	bottom_up_cube(input, cardinalities, 0, input.begin(), input.end());

	write_cuboid_counts(outfile2);

	cout << "done!" << endl;

	return 0;
}
