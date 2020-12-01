#include <string>
#include <mutex>
#include <condition_variable>

#include <kognac/utils.h>

using namespace std;

int main(int argc, const char** argv) {
	int v = 510;
	char buffer[1024];
	int offset = 0;
	Utils::encode_vint2(buffer, offset, v);
	int newOffset = 0;
	int n = Utils::decode_vint2(buffer, &newOffset);
	std::cout << v << " " << n << std::endl;
}
