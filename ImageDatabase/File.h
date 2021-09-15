#pragma once

#include <string>
#include <filesystem>

namespace File
{
	constexpr int Version[]{ 1, 0, 0, 0 };
	
    void ReadToEnd(std::string& out, const std::filesystem::path& path);

	std::string ReadToEnd(const std::filesystem::path& path);
}
