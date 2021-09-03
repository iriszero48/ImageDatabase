#pragma once

#include <filesystem>

namespace File
{
    void ReadToEnd(std::string& out, const std::filesystem::path& path)
    {
		std::ifstream fs(path, std::ios::in | std::ios::binary);
		if (!fs) throw std::runtime_error("[File::ReadToEnd] open file fail");

		constexpr auto fsBufSize = 4096;
		const auto fsBuf = std::make_unique<char[]>(fsBufSize);
		fs.rdbuf()->pubsetbuf(fsBuf.get(), fsBufSize);

		while (!fs.eof())
		{
			constexpr auto bufSize = 4096;
			char buf[bufSize];
			fs.read(buf, bufSize);
			out.append(std::string_view(buf, fs.gcount()));
		}

		fs.close();
    }

    std::string ReadToEnd(const std::filesystem::path& path)
    {
        std::string out{};
        ReadToEnd(out, path);
        return out;
    }
}
