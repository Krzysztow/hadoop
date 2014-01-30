#include <string>
  
#include  "stdint.h"  // <--- to prevent uint64_t errors! 
 
#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
 
using namespace std;

/**
 * Program consists only of mapper, no reducer means no additional network transfers.
 *
 * Mapper reads line by line, and emits empty keys and transformed to upper case lines.
 * Config specifies followings:
 *  - no reducer is used;
 *  - key-value delimiter should be empty string, so that output file reflects the original one but uppercase. 
 *  However when empty string is chosen, Pipes uses default tab separator - thus config file uses space.
 *
 */

class ToUpperMapper : public HadoopPipes::Mapper {
public:
  //constructor: does nothing
  ToUpperMapper( HadoopPipes::TaskContext& context ) {
  }

  //map function: receives a line, outputs (byteOffset, upper(line))
  //byte offset is monotonically rising, so sorting will be achieved
  void map(HadoopPipes::MapContext& context) {
    //get line of text
    string line = context.getInputValue();
	//transform to uppercase
	string::iterator it = line.begin();
	while (it != line.end()) {
		if ('a' <= *it && *it <= 'z') {
			*it += 'A' - 'a';
		}
		++it;
	}
    //emit
    context.emit("", line);
  }
};
 
class ToUpperReducer : public HadoopPipes::Reducer {
public:
  //constructor: does nothing
  ToUpperReducer(HadoopPipes::TaskContext& context) {
  }

  //reduce function - does nothing 
  void reduce(HadoopPipes::ReduceContext& context) {
  }
};

int main(int argc, char *argv[]) {
	return HadoopPipes::runTask(
		HadoopPipes::TemplateFactory<ToUpperMapper, ToUpperReducer >());
}

