#pragma once

#include <cstdint>
#include <filesystem>
#include <utility>
#include <vector>
#include <array>
#include <unordered_set>
#include <execution>
#include <bit>
#include <format>

#include <eigen3/Eigen/Eigen>
#include <opencv2/core.hpp>
#include <opencv2/dnn.hpp>
#include <zip.h>
#include <boost/stacktrace.hpp>
#include <opencv2/highgui.hpp>
#include <nlohmann/json.hpp>

#include "Utility/Utility.hpp"
#include "String/String.hpp"
#include "Log/Log.hpp"
#include "File/File.hpp"
#include "Image/Image.hpp"
#include "Cryptography/Md5.hpp"
#include "Video/Video.hpp"
#include "Thread/Thread.hpp"
#include "Time/Time.hpp"
#include "Bit/Bit.hpp"
#include "Image/File.hpp"

#include "Serialization.hpp"

namespace ImageDatabase
{
	struct LogMsg
	{
		decltype(std::chrono::system_clock::now()) Time;
		decltype(std::this_thread::get_id()) ThreadId;
		std::string Source;
		std::u8string Message;
	};

	static CuLog::Logger<LogMsg> Log;

	namespace Detail
	{
		inline decltype(auto) LogTime(const decltype(LogMsg::Time)& time)
		{
			auto t = std::chrono::system_clock::to_time_t(time);
			tm local{};
			CuTime::Local(&local, &t);
			std::ostringstream ss;
			ss << std::put_time(&local, "%F %X");
			return ss.str();
		}
	}

#define LogImpl(level, ...)\
	if((int)level <= (int)ImageDatabase::Log.Level) ImageDatabase::Log.Write<level>(\
		std::chrono::system_clock::now(),\
		std::this_thread::get_id(),\
		std::string(CuUtil::String::Combine("[", CuUtil_Filename, ":", CuUtil_LineString "] [", __FUNCTION__ , "] ").data()),\
		CuStr::FormatU8(__VA_ARGS__))
#define LogNone(...) LogImpl(CuLog::LogLevel::None, __VA_ARGS__)
#define LogErr(...) LogImpl(CuLog::LogLevel::Error, __VA_ARGS__)
#define LogWarn(...) LogImpl(CuLog::LogLevel::Warn, __VA_ARGS__)
#define LogInfo(...) LogImpl(CuLog::LogLevel::Info, __VA_ARGS__)
#define LogVerb(...) LogImpl(CuLog::LogLevel::Verb, __VA_ARGS__)
#define LogDebug(...) LogImpl(CuLog::LogLevel::Debug, __VA_ARGS__)

	class Exception : public std::runtime_error
	{
		using std::runtime_error::runtime_error;
	};

	class ApiException : public Exception
	{
		using Exception::Exception;
	};

#define ID_MakeExceptImpl(ex, ...) \
	ex(\
		CuStr::Appends(\
			CuUtil::String::Combine(\
				"[", #ex, "] "\
				"[",CuUtil_Filename,":",CuUtil_LineString,"] "\
				"[", __FUNCTION__, "] ").data(),\
			CuStr::Format(__VA_ARGS__), "\n",\
			boost::stacktrace::to_string(boost::stacktrace::stacktrace())))
#define ID_MakeExcept(...) ID_MakeExceptImpl(ImageDatabase::Exception, __VA_ARGS__)
#define ID_MakeApiExcept(api, ...) ID_MakeExceptImpl(ImageDatabase::ApiException, "[{}] {}", api, CuStr::Format(__VA_ARGS__))
	
	namespace Detail
	{
		inline std::string LibzipErrStr(const zip_error_t& err)
		{
			std::string buf;
			
			if (const auto se = err.sys_err; se != 0)
			{
				buf.append("system error: ");
				if (se == 1) buf.append("ZIP_ET_SYS");
				else if (se == 2) buf.append("ZIP_ET_ZLIB");
				else buf.append(*CuConv::ToString(se));
				buf.append(", ");
			}

			buf.append("zip error: ");
			zip_error_t error;
			zip_error_init_with_code(&error, err.zip_err);
			buf.append(zip_error_strerror(&error));
			zip_error_fini(&error);

			return buf;
		}
	}

	CuEnum_MakeEnumDef(Device, cpu, cuda, opencl);
	CuEnum_MakeEnumDef(Decoder, FFmpeg, GraphicsMagick, DirectXTex);

	using PathType = std::vector<char8_t>;
	using Md5Type = std::array<char, 32>;
	using Vgg16Type = Eigen::Matrix<float, 512, 1>;

	using RawMd5Type = std::array<std::uint8_t, 16>;

	struct ImageInfo
	{
		PathType Path{};
		Md5Type Md5{};
		Vgg16Type Vgg16{};

		static std::u8string_view PathU8String(const PathType& path)
		{
			return std::u8string_view(path.data(), path.size());
		}

		static std::string_view Md5String(const Md5Type& md5)
		{
			return std::string_view(md5.data(), md5.size());
		}

		static RawMd5Type RawMd5(const Md5Type& md5)
		{
			RawMd5Type ret{};
			for (size_t i = 0; i < ret.size(); ++i)
			{
				ret[i] = CuConv::FromString<uint8_t>(std::string_view(md5.data() + i * 2, 2), 16).value();
			}
			return ret;
		}

		static std::span<float, 512> Vgg16Span(const Vgg16Type& vgg16)
		{
			return std::span<float, 512>(const_cast<float*>(vgg16.data()), 512);
		}

		static std::string Vgg16String(const Vgg16Type& vgg16)
		{
			return nlohmann::json(Vgg16Span(vgg16)).dump();
		}
	};

	class Extractor
	{
	protected:
		cv::dnn::Net vgg16;

	public:
		explicit Extractor(const Device& dev,
		                   const std::string& protoTxtPath = "vgg16-deploy.prototxt",
		                   const std::string& caffeModelPath = "vgg16.caffemodel")
		{
			const auto loadFile = [](const std::string& file)
			{
				try
				{
					return CuFile::ReadAllBytes(file);
				}
				catch (...)
				{
					std::throw_with_nested(ID_MakeExcept("init model file '{}' failed.", file));
				}
			};

			const auto protoTxt = loadFile(protoTxtPath);
			const auto caffeModel = loadFile(caffeModelPath);

			vgg16 = cv::dnn::readNetFromCaffe(protoTxt, caffeModel);

			if (dev == Device::cpu)
			{
				vgg16.setPreferableBackend(cv::dnn::DNN_BACKEND_VKCOM);
				vgg16.setPreferableTarget(cv::dnn::DNN_TARGET_VULKAN);
			}

			if (dev == Device::cuda)
			{
				vgg16.setPreferableBackend(cv::dnn::DNN_BACKEND_CUDA);
				vgg16.setPreferableTarget(cv::dnn::DNN_TARGET_CUDA);
			}

			if (dev == Device::opencl)
			{
				vgg16.setPreferableBackend(cv::dnn::DNN_BACKEND_DEFAULT);
				vgg16.setPreferableTarget(cv::dnn::DNN_TARGET_OPENCL);
			}
		}

		PathType Path(const std::filesystem::path& path)
		{
			LogVerb("path: {}", path);
			const auto u8str = path.u8string();
			const auto u8size = u8str.length();
			PathType ret(u8size);
			ret.shrink_to_fit();
			std::copy_n(u8str.data(), u8size, ret.data());
			return ret;
		}

		Md5Type Md5(const CuImg::ImageBGR_OpenCV& img)
		{
			CuCrypto::Md5 md5;
			const auto linesize = img.Linesize();
			const auto usedSize = CuImg::CuBGR::ColorSize() * img.Width();
			const auto height = img.Height();
			if (linesize == usedSize)
			{
				md5.Append(img.Data(), img.Size());
			}
			else
			{
				for (size_t i = 0; i < height; ++i)
				{
					md5.Append(img.Data() + linesize * i, usedSize);
				}
			}
			std::string digest = md5.Digest();
			LogVerb("md5: {}", digest);

			Md5Type ret{};
			std::copy_n(digest.data(), ret.size(), ret.data());
			return ret;
		}

		Vgg16Type Vgg16(const cv::Mat& image)
		{
			vgg16.setInput(cv::dnn::blobFromImage(image, 1., cv::Size(224, 224), cv::Scalar(123.68, 116.779, 103.939), false));
			auto feat = vgg16.forward();
			feat = feat / norm(feat);

			Vgg16Type ret{};
			for (auto i = 0; i < 512; ++i)
			{
				ret(i, 0) = feat.at<float>(0, i, 0);
			}

			LogVerb("vgg16: {}, {}, {}, ...", ret(0, 0), ret(1, 0), ret(2, 0));
			return ret;
		}
	};

#define ID_MakeImageUnpackParams(p, m, v) const PathType& p, const Md5Type& m, const Vgg16Type& v
	template <typename T>
	struct IContainer
	{
		[[nodiscard]] size_t Size() const
		{
			return static_cast<T*>(this)->Size();
		}

		void ForEach(std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func)
		{
			static_cast<T*>(this)->ForEach(func);
		}

		void ForEach(std::function<void(ImageInfo&)>&& func)
		{
			static_cast<T*>(this)->ForEach(func);
		}

		void ParallelForEach(std::function<void(ImageInfo&)>&& func)
		{
			static_cast<T*>(this)->ParallelForEach(func);
		}

		void Append(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
			static_cast<T*>(this)->Append(std::move(path), std::move(md5), std::move(vgg16));
		}

		void Get(const size_t idx, std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func, const std::any& param)
		{
			static_cast<T*>(this)->Get(idx, func, param);
		}

		template <typename Func>
		bool DeserializeOne(std::ifstream& fs, Serialization::Deserialize& deSer, Func&& func)
		{
			PathType path;
			try
			{
				auto [p] = deSer.Read<PathType>();
				path = std::move(p);
			}
			catch (Serialization::Eof&)
			{
				return false;
			}
			LogVerb("path: {}", std::u8string_view(path.data(), path.size()));

			Md5Type md5{};
			fs.read(md5.data(), md5.size());
			LogVerb("md5: {}", std::string_view(md5.data(), md5.size()));

			Vgg16Type vgg16;
			fs.read(reinterpret_cast<char*>(vgg16.data()), sizeof(float) * 512);
			if constexpr (std::endian::big == std::endian::native)
			{
				auto ptr = vgg16.data();
				for (auto i = 0; i < 512; ++i)
				{
					ptr[i] = CuBit::ByteSwap(ptr[i]);
				}
			}
			LogVerb("vgg16: {}, {}, {}, ...", vgg16[0], vgg16[1], vgg16[2]);

			func(std::move(path), std::move(md5), std::move(vgg16));
			return true;
		}

		void Deserialize(const std::filesystem::path& dbPath)
		{
			std::ifstream fs(dbPath, std::ios::in | std::ios::binary);
			if (!fs) throw ID_MakeApiExcept("std::ifstream::operator!", "{}", "return false: open database failed");

			Serialization::Deserialize deSer(fs);
			const auto cb = [&](PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
				{
					Append(std::move(path), std::move(md5), std::move(vgg16));
				};
			while (fs && DeserializeOne(fs, deSer, cb)) {}

			fs.close();
		}
	};

	struct MapContainer : public IContainer<MapContainer>
	{
		struct MapContainerHasher
		{
			size_t operator()(const PathType& val) const
			{
				return std::hash<std::u8string_view>{}(std::u8string_view(val.data(), val.size()));
			}
		};

		std::unordered_map<PathType, std::tuple<Md5Type, Vgg16Type>, MapContainerHasher> Data;

		void ForEach(std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func)
		{
			for (const auto& [first, second] : Data)
			{
				const auto& [m, v] = second;
				func(first, m, v);
			}
		}

		void ForEach(std::function<void(ImageInfo&)>&& func) const
		{
			throw ID_MakeExcept("not impl");
		}

		void ParallelForEach(std::function<void(ImageInfo&)>&& func) const
		{
			throw ID_MakeExcept("not impl");
		}

		void Append(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
			Data[path] = std::make_tuple(md5, vgg16);
		}

		[[nodiscard]] size_t Size() const
		{
			return Data.size();
		}

		void Get(const size_t idx, std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func, const std::any& param)
		{
			throw ID_MakeExcept("not impl");
		}
	};

	struct VectorContainer : public IContainer<VectorContainer>
	{
		std::vector<ImageInfo> Data{};

		void ForEach(std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func)
		{
			for (const auto& [p, m, v] : Data)
			{
				func(p, m, v);
			}
		}

		void ForEach(std::function<void(ImageInfo&)>&& func)
		{
			std::ranges::for_each(Data, func);
		}

		void ParallelForEach(std::function<void(ImageInfo&)>&& func)
		{
			std::for_each(std::execution::par_unseq, Data.begin(), Data.end(), func);
		}

		void Append(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
            Data.push_back(ImageInfo{path, md5, vgg16});
		}

		[[nodiscard]] size_t Size() const
		{
			return Data.size();
		}

		void Get(const size_t idx, std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func, const std::any&)
		{
			const auto& [p, m, v] = Data[idx];
			func(p, m, v);
		}
	};

	struct LazyContainer : public IContainer<LazyContainer>
	{
		std::vector<uint64_t> Data{};
		std::filesystem::path DbPath{};

		void ForEach(std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func)
		{
			auto fs = Open();

			Serialization::Deserialize deSer(fs);

			for (const uint64_t offset : Data)
			{
				fs.seekg(offset);
				DeserializeOne(fs, deSer, func);
			}
		}

		[[nodiscard]] std::ifstream Open(const std::filesystem::path& path) const
		{
			std::ifstream fs(path, std::ios::in | std::ios::binary);
			if (!fs) throw ID_MakeApiExcept("std::ifstream::operator!", "{}", "return false: open database failed");
			return fs;
		}

		[[nodiscard]] std::ifstream Open() const
		{
			return Open(DbPath);
		}

		void ForEach(std::function<void(ImageInfo&)>&& func) const
		{
			throw ID_MakeExcept("not impl");
		}

		void ParallelForEach(std::function<void(ImageInfo&)>&& func) const
		{
			throw ID_MakeExcept("not impl");
		}

		void Append(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16) const
		{
			throw ID_MakeExcept("not impl");
		}

		void Deserialize(const std::filesystem::path& dbPath)
		{
			DbPath = dbPath;

			std::ifstream fs(dbPath, std::ios::in | std::ios::binary);
			if (!fs) throw ID_MakeApiExcept("std::ifstream::operator!", "{}", "return false: open database failed");

			Serialization::Deserialize deSer(fs);
			while (fs)
			{
				PathType path;
				try
				{
					auto [pSize] = deSer.Read<uint64_t>();
					const uint64_t pos = fs.tellg();
					Data.push_back(pos - sizeof(uint64_t));
					fs.seekg(pos + pSize + 32 + sizeof(float) * 512);
				}
				catch (Serialization::Eof&)
				{
					break;
				}
			}

			fs.close();
		}

		[[nodiscard]] size_t Size() const
		{
			return Data.size();
		}

		void Get(const size_t idx, std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func, const std::any& param)
		{
			auto* fs = std::any_cast<std::ifstream*>(param);
			const auto offset = Data[idx];
			fs->seekg(offset);
			Serialization::Deserialize deSer(*fs);
			DeserializeOne(*fs, deSer, func);
		}
	};

	class Reader
	{
	public:
		struct MemoryType
		{
			std::filesystem::path Path;
			std::vector<uint8_t> Data;
		};
		using PathType = std::filesystem::path;
		using LoadType = std::variant<PathType, MemoryType>;

		using HandlerType = std::function<void(Reader&, LoadType&&)>;
	private:
		std::vector<std::filesystem::path> buildPaths{};
		HandlerType handler{};

	public:
		std::unordered_set<std::u8string> ExtBlackList{};
		std::unordered_map<std::u8string, Decoder> ExtDecoderList{};
		std::unordered_set<std::u8string> ZipList{u8".zip", u8".7z", u8".rar", u8".gz", u8".xz"};
		
		Reader() = delete;

		Reader(HandlerType&& handler , std::vector<std::filesystem::path> paths) : buildPaths(std::move(paths)), handler(std::move(handler)) {}

		void Read()
		{
			LoadPaths(buildPaths);
		}

	private:
		void LoadPaths(const std::vector<std::filesystem::path>& paths)
		{
			for (const auto& p : paths) LoadPath(p);
		}

		void LoadPath(const std::filesystem::path& path)
		{
			if (is_directory(path))
			{
				LoadDirPath(path);
			}
			else
			{
				LoadFilePath(path);
			}
		}

		void LoadDirPath(const std::filesystem::path& path)
		{
			std::error_code ec{};
			const std::error_code noErr{};
			for (auto& f : std::filesystem::recursive_directory_iterator(path, ec))
			{
				if (ec != noErr)
				{
					LogWarn("{}: {}", f.path(), ec.message());
					ec.clear();
					continue;
				}
				
				if (!f.is_directory())
				{
					try
					{
						LoadFilePath(f.path());
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}", f.path(), ex.what());
					}
				}
			}
		}

		void LoadFilePath(const std::filesystem::path& path)
		{
			const auto ext = CuStr::ToLowerU8(path.extension().u8string());
			if (ExtBlackList.contains(ext)) return;

			if (ZipList.contains(ext))
			{
				LoadZip(path);
			}
			else
			{
				LoadFile(path);
			}
		}

		void LoadZip(const std::filesystem::path& path)
		{
			const auto zipFile = CuFile::ReadAllBytes(path);

			zip_error_t error;
			zip_source_t* src = zip_source_buffer_create(zipFile.data(), zipFile.size(), 0, &error);
			if (src == nullptr)
				throw ID_MakeApiExcept("zip_source_buffer_create", "{}", Detail::LibzipErrStr(error));

			zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &error);
			if (za == nullptr)
				throw ID_MakeApiExcept("zip_open_from_source", "{}", Detail::LibzipErrStr(error));

			const auto entries = zip_get_num_entries(za, 0);
			if (entries < 0)
			{
				const auto* err = zip_get_error(za);
				const auto msg = Detail::LibzipErrStr(*err);
				zip_close(za);
				throw ID_MakeApiExcept("zip_get_num_entries", "{}", msg);
			}

			for (int i = 0; i < entries; ++i)
			{
				struct zip_stat zs {};
				if (zip_stat_index(za, i, 0, &zs) == 0)
				{
					if (const std::string_view filename(zs.name); filename[filename.length() - 1] != '/')
					{
						auto* const zf = zip_fopen_index(za, i, 0);
						if (zf == nullptr)
						{
							const auto err = zip_get_error(za);
							LogWarn("{}({}): [zip_fopen_index] {}", path, filename, Detail::LibzipErrStr(*err));
							continue;
						}

						MemoryType buf{};

						buf.Data = std::vector<uint8_t>(zs.size, 0);
						if (const auto ret = zip_fread(zf, buf.Data.data(), zs.size); ret < 0)
						{
							const auto err = zip_get_error(za);
							LogWarn("{}({}): [zip_fread] return {}: {}", path, filename, ret, Detail::LibzipErrStr(*err));
							zip_fclose(zf);
							continue;
						}

						buf.Path = path / filename;
						LoadFile(std::move(buf));

						zip_fclose(zf);
					}
				}
				else
				{
					const auto err = zip_get_error(za);
					LogWarn("{}({}): [zip_stat_index] {}", path, i, Detail::LibzipErrStr(*err));
				}
			}
			zip_close(za);
		}

		void LoadFile(LoadType&& data)
		{
			handler(*this, std::move(data));
		}

		template <typename SetFunc>
		static void ProcessWithFFmpeg(SetFunc&& set, Extractor& extractor, const std::filesystem::path& filePath, const std::span<const uint8_t>& fileData)
		{
				CuVid::DecoderBGR decoder{};
				if (!fileData.empty())
				{
					decoder.Config.Input = fileData;
				}
				else
				{
					decoder.Config.Input = filePath;
				}

				size_t passed = 0;
				decoder.Config.VideoHandler = [&](const CuImg::ImageBGR_Ref& raw)
				{
					auto img = CuImg::ConvertToImageBGR_OpenCV(raw);

#if 0
					{
						std::string t{};
						try
						{
							t = String::ToString(filePath);
						}
						catch (...)
						{
							t = String::ToDirtyUtf8String(filePath.u8string());
						}
						cv::imshow(t, img.Raw());
						cv::waitKey();
				}
#endif
					const auto idx = decoder.GetVideoCodecContext()->frame_number - 1;

					const auto pu8 = idx < 1 ? filePath : filePath / *CuConv::ToString(idx);
					set(extractor.Path(pu8), extractor.Md5(img), extractor.Vgg16(img.Raw()));
					++passed;
		};

				decoder.LoadFile();
				decoder.FindStream();
				while (!decoder.Eof())
				{
					try
					{
						decoder.Read();
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}", filePath, ex.what());
					}
				}
				if (passed == 0) throw ID_MakeExcept("{}", "nop");
		}

		template <typename SetFunc>
		static void ProcessWithGraphicsMagick(SetFunc&& set, Extractor& extractor, const std::filesystem::path& filePath, const std::span<const uint8_t>& fileData)
		{
			std::string mgPath{};
			Magick::Blob mgData{};
			if (fileData.empty())
			{
				try
				{
					mgPath = filePath.string();
				}
				catch (...)
				{
					uint8_t* ptr;
					size_t size;
					CuFile::ReadAllBytesAsPtr(filePath, &ptr, size);
					mgData.updateNoCopy(ptr, size);
				}
			}
			else
			{
				mgData.update(fileData.data(), fileData.size_bytes());
			}
			
			std::vector<Magick::Image> raw{};
			if (!mgPath.empty())
			{
				Magick::readImages(&raw, mgPath);
			}
			else
			{
				Magick::readImages(&raw, mgData);
			}

			for (size_t i = 0; i < raw.size(); ++i)
			{
				CuImg::DiscreteImageRGBOpacity16_GraphicsMagick buf{};
				buf.Raw() = raw[i];

				auto img = CuImg::ConvertToImageBGR_OpenCV(buf);
				const auto pu8 = (i < 1 ? filePath : filePath / *CuConv::ToString(i));
				set(extractor.Path(pu8), extractor.Md5(img), extractor.Vgg16(img.Raw()));
			}
		}

		template <typename SetFunc>
		static void ProcessWithDirectXTex(SetFunc&& set, Extractor& extractor, const std::filesystem::path& filePath, const std::span<const uint8_t>& fileData)
		{
#ifdef _MSC_VER
			std::filesystem::path dxPath;
			bool useTmp = false;
			if (fileData.empty())
			{
				dxPath = filePath;
			}
			else
			{
				static auto tmpDir = []()
				{
					const auto tmpDir = std::filesystem::temp_directory_path() / "ImageDatabase";
					if (!exists(tmpDir)) std::filesystem::create_directory(tmpDir);
					return tmpDir;
				}();

				dxPath = tmpDir / CuStr::FormatU8("{}{}", CuStr::Combine(std::hex, std::this_thread::get_id()), filePath.extension());
				CuFile::WriteAllBytes(dxPath, fileData.data(), fileData.size_bytes());
				useTmp = true;
			}
			auto raw = CuImg::LoadFile_ImageRGBA_DirectXTex(dxPath);
			const auto count = raw.Raw().GetImageCount();
			for (size_t i = 0; i < count; ++i)
			{
				auto& ri = raw.Raw().GetImages()[i];

				auto img = CuImg::ConvertToImageBGR_OpenCV(CuImg::ConvertToConstRef_RGBA(ri.pixels, ri.width, ri.height, ri.rowPitch));
				const auto pu8 = (i < 1 ? filePath : filePath / *CuConv::ToString(i));
				set(extractor.Path(pu8), extractor.Md5(img), extractor.Vgg16(img.Raw()));
			}
			if (useTmp) std::filesystem::remove(dxPath);
#else
            throw ID_MakeExcept("{}", "not impl");
#endif
		}

	public:
		template <typename SetFunc>
		void Process(SetFunc&& set, Extractor& extractor, const LoadType& data)
		{
			std::filesystem::path filePath{};
			std::span<const uint8_t> fileData{};

			std::visit(CuUtil::Variant::Visitor{
				[&](const PathType& val)
				{
					filePath = val;
				},
					[&](const MemoryType& val)
				{
					fileData = std::span(val.Data);
					filePath = val.Path;
				}
			}, data);

			const auto ext = CuStr::ToLowerU8(filePath.extension().u8string());
			std::optional<Decoder> specDec{};
			if (const auto p = ExtDecoderList.find(ext); p != ExtDecoderList.end()) specDec = p->second;

			for (constexpr auto decoders = std::array{Decoder::FFmpeg, Decoder::GraphicsMagick, Decoder::DirectXTex}; const auto decoder : decoders)
			{
				if (specDec)
				{
					if (*specDec != decoder) continue;
				}
				LogVerb("using decoder: {}", CuEnum::ToString(decoder));
				if (decoder == Decoder::FFmpeg)
				{
					try
					{
						ProcessWithFFmpeg(set, extractor, filePath, fileData);
						return;
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}: {}", CuEnum::ToString(decoder), filePath, ex.what());
					}
				}
				else if (decoder == Decoder::GraphicsMagick)
				{
					try
					{
						ProcessWithGraphicsMagick(set, extractor, filePath, fileData);
						return;
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}: {}", CuEnum::ToString(decoder), filePath, ex.what());
					}
				}
				else if (decoder == Decoder::DirectXTex)
				{
					try
					{
						ProcessWithDirectXTex(set, extractor, filePath, fileData);
						return;
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}: {}", CuEnum::ToString(decoder), filePath, ex.what());
					}
				}
				else
				{
					CuUtil_Assert(false, Exception);
				}
			}
		}
	};

	template<typename Containor = MapContainer>
	class Database
	{
	public:
		Database() = default;

		explicit Database(const std::filesystem::path& path)
		{
			databasePath = path;
			Load(databasePath);
		}

		void Load(const std::filesystem::path& dbPath)
		{
			Images.Deserialize(dbPath);
		}

		static void SaveImage(std::ofstream& fs, const ImageInfo& img)
		{
			SaveImage(fs, img.Path, img.Md5, img.Vgg16);
		}

		static void SaveImage(std::ofstream& fs, const PathType& p, const Md5Type& m, const Vgg16Type& v)
		{
			Serialization::Serialize ser(fs);
			ser.Write(p);
			fs.write(m.data(), 32);
			fs.write(reinterpret_cast<const char*>(v.data()), sizeof(float) * 512);
		}

		static std::ofstream CreateDatabaseFile(const std::filesystem::path& path)
		{
			std::ofstream fs(path, std::ios::out | std::ios::binary);
			if (!fs) throw std::runtime_error("load file: bad stream");

			// const auto fsBuf = std::make_unique<char[]>(4096);
			// fs.rdbuf()->pubsetbuf(fsBuf.get(), 4096);

			return fs;
		}

		void Save()
		{
			Save(databasePath);
		}

		void Save(std::ofstream& fs)
		{
			Images.ForEach([&](const PathType& p, const Md5Type& m, const Vgg16Type& v)
				{
					SaveImage(fs, p, m, v);
				});

			fs.close();
		}
		
		void Save(const std::filesystem::path& savePath)
		{
			auto fs = CreateDatabaseFile(savePath);
			Save(fs);
		}

		void ForEach(std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func)
		{
			Images.ForEach(std::move(func));
		}

		void ForEach(std::function<void(ImageInfo&)>&& func)
		{
			Images.ForEach(std::move(func));
		}

		void ParallelForEach(std::function<void(ImageInfo&)>&& func)
		{
			Images.ParallelForEach(std::move(func));
		}

		void Append(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
			Images.Append(std::move(path), std::move(md5), std::move(vgg16));
		}

		template <typename Func>
		bool DeserializeOne(std::ifstream& fs, Serialization::Deserialize& deSer, Func&& func)
		{
			return Images.DeserializeOne(fs, deSer, std::move(func));
		}

		void Deserialize(const std::filesystem::path& dbPath)
		{
			Images.Deserialize(dbPath);
		}

		[[nodiscard]] size_t Size() const
		{
			return Images.Size();
		}

		void Get(const size_t idx, std::function<void(const PathType&, const Md5Type&, const Vgg16Type&)>&& func, const std::any& param)
		{
			Images.Get(idx, std::move(func), param);
		}
	private:
		std::filesystem::path databasePath;

	public:
		Containor Images{};
	};
}

CuEnum_MakeEnumSpec(ImageDatabase, Device);
CuEnum_MakeEnumSpec(ImageDatabase, Decoder);
