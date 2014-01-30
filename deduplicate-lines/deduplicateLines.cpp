#include <string>
  
#include  "stdint.h"  // <--- to prevent uint64_t errors! 
 
#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
 
using namespace std;
/**
 * Program deduplicates sentences from the input folder.
 * 
 * Mapper reads files line by line, and emits (line, "") pairs
 * Reducer input is sorted by key, so duplicates follow each other. Thus reducer emits only those keys
 * that are not equal to the previous ones -> onyly the first duplicate sequence is emitted.
 *
 * As an optimization, a combiner (same as reducer) is used. This reduces the network traffic. 
 * No in-map combiner is implemented, since it's probable that most of the text is not duplicated, 
 * thus memory limits would be an issue (dictionary of all encountered sentences would 
 * have to be kept in memory).
 */


class DeduplicationMapper : public HadoopPipes::Mapper {
public:
  //constructor: does nothing
  DeduplicationMapper(HadoopPipes::TaskContext& context) {
  }

  //map function: receives a line, outputs (line, "")
  void map(HadoopPipes::MapContext& context) {
    context.emit(context.getInputValue(), "");
  }
};

class DeduplicationReducer: public HadoopPipes::Reducer {
public:

  //constructor: does nothing
  DeduplicationReducer(HadoopPipes::TaskContext& context) {
  }

  /**
 * reduce function - reducer is invoked for each key (sentence in our case) - 
 * - it's enough to emit the sentence once for invocation
 */
  void reduce(HadoopPipes::ReduceContext& context) {
	//looks like a sanity check (should have at least one value for a key, but
	//it's said that without ReduceContext::nextValue(), key is invalid
    if (context.nextValue()) {
		context.emit(context.getInputKey(), "");
	}
  }
};

//there is no template for a factory with mapper, reducer and combiner, thus one is defined here
class CombinerFactory: public HadoopPipes::Factory {
public:
	virtual HadoopPipes::Mapper* createMapper(HadoopPipes::MapContext& context) const {
		return new DeduplicationMapper(context);
	}

	virtual HadoopPipes::Reducer* createReducer(HadoopPipes::ReduceContext& context) const {
		return new DeduplicationReducer(context);
	}

	virtual HadoopPipes::Reducer* createCombiner(HadoopPipes::MapContext& context) const {
		return new DeduplicationReducer(context);
	}
};

int main(int argc, char *argv[]) {
	return HadoopPipes::runTask(
		CombinerFactory());
}

