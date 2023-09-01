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

// extern "C"
// {
// #include <zip.h>
// 	
// #include <libavformat/avformat.h>
// #include <libavcodec/avcodec.h>
// #include <libavutil/avutil.h>
// #include <libavutil/imgutils.h>
// #include <libswscale/swscale.h>
// }
// 
// #include "Cryptography.h"
// #include "String.h"
// #include "File.h"
#include "Serialization.hpp"
// #include "Bit.h"
// #include "Thread.h"

namespace ImageDatabase
{
	struct LogMsg
	{
		decltype(std::chrono::system_clock::now()) Time;
		decltype(std::this_thread::get_id()) ThreadId;
		std::string Source;
		std::u8string Message;
	};

	static Logger<LogMsg> Log;

	namespace Detail
	{
		decltype(auto) LogTime(const decltype(LogMsg::Time)& time)
		{
			auto t = std::chrono::system_clock::to_time_t(time);
			tm local{};
			Time::Local(&local, &t);
			std::ostringstream ss;
			ss << std::put_time(&local, "%F %X");
			return ss.str();
		}
	}

#define LogImpl(level, ...)\
	if((int)level <= (int)ImageDatabase::Log.Level) ImageDatabase::Log.Write<level>(\
		std::chrono::system_clock::now(),\
		std::this_thread::get_id(),\
		std::string(CuUtility::String::Combine("[", CuUtility_Filename, ":", CuUtility_LineString "] [", __FUNCTION__ , "] ").data()),\
		String::FormatU8(__VA_ARGS__))
#define LogNone(...) LogImpl(LogLevel::None, __VA_ARGS__)
#define LogErr(...) LogImpl(LogLevel::Error, __VA_ARGS__)
#define LogWarn(...) LogImpl(LogLevel::Warn, __VA_ARGS__)
#define LogInfo(...) LogImpl(LogLevel::Info, __VA_ARGS__)
#define LogVerb(...) LogImpl(LogLevel::Verb, __VA_ARGS__)
#define LogDebug(...) LogImpl(LogLevel::Debug, __VA_ARGS__)

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
		String::Appends(\
			CuUtility::String::Combine(\
				"[", #ex, "] "\
				"[",CuUtility_Filename,":",CuUtility_LineString,"] "\
				"[", __FUNCTION__, "] ").data(),\
			String::Format(__VA_ARGS__), "\n",\
			boost::stacktrace::to_string(boost::stacktrace::stacktrace())))
#define ID_MakeExcept(...) ID_MakeExceptImpl(ImageDatabase::Exception, __VA_ARGS__)
#define ID_MakeApiExcept(api, ...) ID_MakeExceptImpl(ImageDatabase::ApiException, "[{}] {}", api, std::format(__VA_ARGS__))
	
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
				else buf.append(*Convert::ToString(se));
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

	MakeEnumDef(Device, cpu, cuda, opencl);
	MakeEnumDef(Decoder, FFmpeg, GraphicsMagick, DirectXTex);

	using PathType = std::vector<char8_t>;
	using Md5Type = std::array<char, 32>;
	using Vgg16Type = Eigen::Matrix<float, 512, 1>;

	struct ImageInfo
	{
		PathType Path;
		Md5Type Md5;
		Vgg16Type Vgg16;
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
					return File::ReadAllBytes(file);
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
			Crypto::Md5 md5;
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

	template <typename T>
	struct IContainer
	{
		[[nodiscard]] decltype(auto) Get(const PathType& key)
		{
			return static_cast<T*>(this)->Get(key);
		}

		void Set(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
			static_cast<T*>(this)->Set(path, md5, vgg16);
		}
	};

	struct MapContainerHasher
	{
		size_t operator()(const PathType& val) const
		{
			return std::hash<std::u8string_view>{}(std::u8string_view(val.data(), val.size()));
		}
	};

	struct MapContainer : public IContainer<MapContainer>
	{
	public:
		std::unordered_map<PathType, std::tuple<Md5Type, Vgg16Type>, MapContainerHasher> Data;

		[[nodiscard]] ImageInfo Get(const PathType& key) const
		{
			const auto d = Data.find(key);
			return { d->first, std::get<0>(d->second), std::get<1>(d->second) };
		}

		void Set(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
			Data[path] = std::make_tuple(md5, vgg16);
		}
	};

	struct VectorContainer : public IContainer<VectorContainer>
	{
		std::vector<ImageInfo> Data{};

		[[nodiscard]] ImageInfo& Get(const PathType& key)
		{
			const auto it = std::find_if(std::execution::par_unseq, Data.begin(), Data.end(), [&](const auto& val) { return val.Path == key; });
			return *it;
		}

		void Set(PathType&& path, Md5Type&& md5, Vgg16Type&& vgg16)
		{
            Data.push_back(ImageInfo{path, md5, vgg16});
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

		using HandlerType = std::function<void(LoadType&&)>;
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
			const auto ext = String::ToLowerU8(path.extension().u8string());
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
			const auto zipFile = File::ReadAllBytes(path);

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

		void LoadFile(LoadType&& data) const
		{
			handler(std::move(data));
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

					const auto pu8 = idx < 1 ? filePath : filePath / *Convert::ToString(idx);
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
					File::ReadAllBytesAsPtr(filePath, &ptr, size);
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
				const auto pu8 = (i < 1 ? filePath : filePath / *Convert::ToString(i));
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

				dxPath = tmpDir / String::FormatU8("{}{}", String::FromStream(std::this_thread::get_id(), std::hex), filePath.extension());
				File::WriteAllBytes(dxPath, fileData.data(), fileData.size_bytes());
				useTmp = true;
			}
			auto raw = CuImg::LoadFile_ImageRGBA_DirectXTex(dxPath);
			const auto count = raw.Raw().GetImageCount();
			for (size_t i = 0; i < count; ++i)
			{
				auto& ri = raw.Raw().GetImages()[i];

				auto img = CuImg::ConvertToImageBGR_OpenCV(CuImg::ConvertToConstRef_RGBA(ri.pixels, ri.width, ri.height, ri.rowPitch));
				const auto pu8 = (i < 1 ? filePath : filePath / *Convert::ToString(i));
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

			std::visit(CuUtility::Variant::Visitor{
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

			const auto ext = String::ToLowerU8(filePath.extension().u8string());
			std::optional<Decoder> specDec{};
			if (const auto p = ExtDecoderList.find(ext); p != ExtDecoderList.end()) specDec = p->second;

			for (constexpr auto decoders = std::array{Decoder::FFmpeg, Decoder::GraphicsMagick, Decoder::DirectXTex}; const auto decoder : decoders)
			{
				if (specDec)
				{
					if (*specDec != decoder) continue;
				}
				LogVerb("using decoder: {}", Enum::ToString(decoder));
				if (decoder == Decoder::FFmpeg)
				{
					try
					{
						ProcessWithFFmpeg(set, extractor, filePath, fileData);
						return;
					}
					catch (const std::exception& ex)
					{
						LogErr("{}: {}: {}", Enum::ToString(decoder), filePath, ex.what());
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
						LogErr("{}: {}: {}", Enum::ToString(decoder), filePath, ex.what());
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
						LogErr("{}: {}: {}", Enum::ToString(decoder), filePath, ex.what());
					}
				}
				else
				{
					CuUtility_Assert(false, Exception);
				}
			}
		}

	private:
		// [[nodiscard]] Image ProcImage(const std::filesystem::path& path, const std::string& buf, const int lineSize) const
		// {			
		// 	Image out{};
		// 	out.Path = path;
		// 	if (log != nullptr)
		// 		log(path.u8string());
		// 
		// 	Cryptography::Md5 md5{};
		// 	md5.Append((uint8_t*)buf.data(), buf.length());
		// 	const std::string md5Str = md5.Digest();
		// 	std::copy_n(md5Str.begin(), 32, out.Md5.begin());
		// 	if (log != nullptr) log(md5Str);
		// 
		// 	const cv::Mat image(224, 224, CV_8UC3, (void*)buf.data(), lineSize);
		// 	//cv::imshow("", image);
		// 	//cv::waitKey(0);
		// 	extractor.Extract(out.Vgg16, image);
		// 	if (log != nullptr) log(*Convert::ToString(out.Vgg16(0, 0)));
		// 
		// 	return out;
		// }
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
			std::ifstream fs(dbPath, std::ios::in | std::ios::binary);
			if (!fs) throw ID_MakeApiExcept("std::ifstream::operator!","{}", "return false: open database failed");

			Serialization::Deserialize deSer(fs);
			while (fs)
			{
				PathType path;
				try
				{
					auto [p] = deSer.Read<PathType>();
					path = std::move(p);
				}
				catch (Serialization::Eof&)
				{
					break;
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
						ptr[i] = Bit::EndianSwap(ptr[i]);
					}
				}
				LogVerb("vgg16: {}, {}, {}, ...", vgg16[0], vgg16[1], vgg16[2]);

				Images.Set(std::move(path), std::move(md5), std::move(vgg16));
			}

			fs.close();
		}

		static void SaveImage(std::ofstream& fs, const ImageInfo& img)
		{
			Serialization::Serialize ser(fs);
			const auto& [path, md5, vgg16] = img;
			ser.Write(path);
			fs.write(md5.data(), 32);
			fs.write(reinterpret_cast<const char*>(vgg16.data()), sizeof(float) * 512);
		}

		void Save()
		{
			Save(databasePath);
		}
		
		void Save(const std::filesystem::path& savePath)
		{
			std::filesystem::remove(savePath);
			
			std::ofstream fs(savePath, std::ios::out | std::ios::binary);
			if (!fs) throw std::runtime_error("load file: bad stream");
			const auto fsBuf = std::make_unique<char[]>(4096);
			fs.rdbuf()->pubsetbuf(fsBuf.get(), 4096);

			if constexpr (std::is_same_v<Containor, MapContainer>)
			{
				for (const auto& [k, v] : Images.Data)
				{
					SaveImage(fs, ImageInfo{ k, std::get<0>(v), std::get<1>(v) });
				}
			}
			else
			{
				for (const auto& img : Images.Data)
				{
					SaveImage(fs, img);
				}
			}

			fs.close();
		}
	private:
		std::filesystem::path databasePath;

	public:
		Containor Images{};
	};
}

MakeEnumSpec(ImageDatabase, Device);
MakeEnumSpec(ImageDatabase, Decoder);
