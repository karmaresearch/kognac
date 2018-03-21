#include <zstr/zstr.hpp>

#include <iostream>

int main(int argc, const char** argv) {
	{
		zstr::ofstream fout("test.gz");
		std::string text("Hi!\n");
		fout.write(text.c_str(), text.size());
	}

	{
		zstr::ifstream fin("test.gz");
		std::string text;
		char buffer[128];
		fin.read(buffer, 4);
		std::cout << buffer << std::endl;
		//fin >> text;
		//std::cout << text << std::endl;
	}
}